use log::{error, info};
use raft::eraftpb::Message;
use raft::prelude::EntryType;
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use std::sync::Mutex;

use crate::data_storage::DataStorage;
use crate::file_handler::file_request_handler;
use crate::generated::{
    get_root_as_generic_request, ErrorResponse, ErrorResponseArgs, GenericRequest, ResponseType,
};
use crate::metadata_storage::MetadataStorage;
use crate::peer_client::PeerClient;
use crate::storage_node::LocalContext;
use crate::utils::{finalize_response, is_write_request};
use flatbuffers::FlatBufferBuilder;
use futures::future::ok;
use futures::sync::oneshot;
use futures::sync::oneshot::Sender;
use futures::Future;
use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;

pub struct RaftManager {
    raft_node: Mutex<RawNode<MemStorage>>,
    pending_responses: Mutex<
        HashMap<
            u128,
            (
                FlatBufferBuilder<'static>,
                Sender<FlatBufferBuilder<'static>>,
            ),
        >,
    >,
    sync_requests: Mutex<Vec<(u64, Sender<()>)>>,
    leader_requests: Mutex<Vec<Sender<u64>>>,
    applied_index: Mutex<u64>,
    peers: HashMap<u64, PeerClient>,
    node_id: u64,
    context: LocalContext,
    data_storage: DataStorage,
    metadata_storage: MetadataStorage,
}

impl RaftManager {
    pub fn new(context: LocalContext) -> RaftManager {
        let node_id = context.node_id;
        let mut peer_ids: Vec<u64> = context
            .peers
            .iter()
            // TODO: huge hack. Assume the port is the node id
            .map(|peer| u64::from(peer.port()))
            .collect();
        peer_ids.push(node_id);

        let raft_config = Config {
            id: node_id,
            peers: peer_ids.clone(),
            learners: vec![],
            election_tick: 10 * 3,
            heartbeat_tick: 3,
            // TODO: need to restore this from storage
            applied: 0,
            max_size_per_msg: 1024 * 1024 * 1024,
            max_inflight_msgs: 256,
            tag: format!("peer_{}", node_id).to_string(),
            ..Default::default()
        };
        let raft_storage = MemStorage::new();
        let raft_node = RawNode::new(&raft_config, raft_storage, vec![]).unwrap();

        RaftManager {
            raft_node: Mutex::new(raft_node),
            pending_responses: Mutex::new(HashMap::new()),
            leader_requests: Mutex::new(vec![]),
            sync_requests: Mutex::new(vec![]),
            applied_index: Mutex::new(0),
            peers: context
                .peers
                .iter()
                .map(|peer| (u64::from(peer.port()), PeerClient::new(*peer)))
                .collect(),
            node_id,
            context: context.clone(),
            data_storage: DataStorage::new(node_id, &peer_ids, &context),
            metadata_storage: MetadataStorage::new(),
        }
    }

    // TODO: remove this method
    pub fn data_storage(&self) -> &DataStorage {
        &self.data_storage
    }

    // TODO: remove this method
    pub fn metadata_storage(&self) -> &MetadataStorage {
        &self.metadata_storage
    }

    pub fn apply_messages(&self, messages: &[Message]) -> raft::Result<()> {
        {
            let mut raft_node = self.raft_node.lock().unwrap();

            for message in messages {
                assert_eq!(message.to, self.node_id);
                raft_node.step(message.clone())?;
            }
        }

        {
            self.process_raft_queue();
        }

        Ok(())
    }

    fn send_outgoing_raft_messages(&self, messages: Vec<Message>) {
        for message in messages {
            let peer = &self.peers[&message.to];
            // TODO: errors
            tokio::spawn(peer.send_raft_message(message));
        }
    }

    pub fn get_latest_local_commit(&self) -> u64 {
        let raft_node = self.raft_node.lock().unwrap();

        if raft_node.raft.leader_id == self.node_id {
            return *self.applied_index.lock().unwrap();
        }

        unreachable!();
    }

    pub fn get_latest_commit_from_leader(&self) -> impl Future<Item = u64, Error = ()> {
        let raft_node = self.raft_node.lock().unwrap();

        let commit: Box<Future<Item = u64, Error = ()> + Send>;
        if raft_node.raft.leader_id == self.node_id {
            commit = Box::new(ok(*self.applied_index.lock().unwrap()));
        } else if raft_node.raft.leader_id == 0 {
            // TODO: wait for a leader
            commit = Box::new(ok(0));
        } else {
            let leader = &self.peers[&raft_node.raft.leader_id];
            commit = Box::new(leader.get_latest_commit());
        }

        commit
    }

    #[allow(clippy::useless_let_if_seq)]
    pub fn get_leader(&self) -> impl Future<Item = u64, Error = ()> {
        let raft_node = self.raft_node.lock().unwrap();

        let leader: Box<Future<Item = u64, Error = ()> + Send>;
        if raft_node.raft.leader_id > 0 {
            leader = Box::new(ok(raft_node.raft.leader_id));
        } else {
            let (sender, receiver) = oneshot::channel();
            self.leader_requests.lock().unwrap().push(sender);
            leader = Box::new(receiver.map_err(|_| ()));
        }

        leader
    }

    // Wait until the given index has been committed
    pub fn sync(&self, index: u64) -> impl Future<Item = (), Error = ()> {
        // Make sure we have the lock on all data structures
        let _raft_node_locked = self.raft_node.lock().unwrap();

        let commit: Box<Future<Item = (), Error = ()> + Send> =
            if *self.applied_index.lock().unwrap() >= index {
                Box::new(ok(()))
            } else {
                let (sender, receiver) = oneshot::channel();
                self.sync_requests.lock().unwrap().push((index, sender));
                Box::new(receiver.map_err(|_| ()))
            };

        commit
    }

    // Should be called once every 100ms to handle background tasks
    pub fn background_tick(&self) {
        {
            let mut raft_node = self.raft_node.lock().unwrap();
            raft_node.tick();

            let leader_id = raft_node.raft.leader_id;
            if leader_id > 0 {
                self.leader_requests
                    .lock()
                    .unwrap()
                    .drain(..)
                    .for_each(|sender| sender.send(leader_id).unwrap());
            }
        }
        // TODO: should be able to only do this on ready, I think
        self.process_raft_queue();
    }

    fn process_raft_queue(&self) {
        let messages = self._process_raft_queue().unwrap();
        self.send_outgoing_raft_messages(messages);
    }

    // Returns the last applied index
    fn _process_raft_queue(&self) -> raft::Result<Vec<Message>> {
        let mut raft_node = self.raft_node.lock().unwrap();

        if !raft_node.has_ready() {
            return Ok(vec![]);
        }

        let mut ready = raft_node.ready();

        if !raft::is_empty_snap(ready.snapshot()) {
            raft_node
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())?;
        }

        if !ready.entries().is_empty() {
            raft_node.mut_store().wl().append(ready.entries())?;
        }

        if let Some(hard_state) = ready.hs() {
            raft_node.mut_store().wl().set_hardstate(hard_state.clone());
        }

        let mut applied_index = *self.applied_index.lock().unwrap();
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                // TODO: probably need to save the term too
                applied_index = max(applied_index, entry.index);

                if entry.data.is_empty() {
                    // New leaders send empty entries
                    continue;
                }

                assert_eq!(entry.entry_type, EntryType::EntryNormal);

                let mut pending_responses = self.pending_responses.lock().unwrap();

                let request = get_root_as_generic_request(&entry.data);
                let mut uuid = [0; 16];
                uuid.copy_from_slice(&entry.context[0..16]);
                if let Some((builder, sender)) =
                    pending_responses.remove(&u128::from_le_bytes(uuid))
                {
                    // TODO: dangerous. wait() could block!
                    match file_request_handler(
                        request,
                        &self.data_storage,
                        &self.metadata_storage,
                        &self.context,
                        builder,
                    )
                    .wait()
                    {
                        Ok(builder) => sender.send(builder).ok().unwrap(),
                        // TODO: handle this somehow. If not all nodes failed, then the filesystem
                        // is probably corrupted, since some will have applied the write, but not all
                        // There should only be a few types of messages that can fail here. truncate is one,
                        // since you can call it with LONG_MAX or some other value that balloons
                        // the message into a huge write. Probably most other messages can't fail
                        Err(error_code) => {
                            error!(
                                "Commit failed {:?} {:?}",
                                error_code,
                                request.request_type()
                            );
                            let mut builder = FlatBufferBuilder::new();
                            let args = ErrorResponseArgs { error_code };
                            let response_offset =
                                ErrorResponse::create(&mut builder, &args).as_union_value();
                            finalize_response(
                                &mut builder,
                                ResponseType::ErrorResponse,
                                response_offset,
                            );
                            sender.send(builder).ok().unwrap()
                        }
                    }
                } else {
                    let builder = FlatBufferBuilder::new();
                    // TODO: pass None for builder to avoid this useless allocation
                    // TODO: dangerous. wait() could block!
                    if let Err(err) = file_request_handler(
                        request,
                        &self.data_storage,
                        &self.metadata_storage,
                        &self.context,
                        builder,
                    )
                    .wait()
                    {
                        // TODO: handle this somehow. If not all nodes failed, then the filesystem
                        // is probably corrupted, since some will have applied the write, but not all.
                        // There should only be a few types of messages that can fail here. truncate is one,
                        // since you can call it with LONG_MAX or some other value that balloons
                        // the message into a huge write. Probably most other messages can't fail
                        error!("Commit failed {:?} {:?}", err, request.request_type());
                    }
                }

                info!(
                    "Committed write index {} (leader={}): {:?}",
                    entry.index,
                    raft_node.raft.leader_id,
                    request.request_type()
                );
            }
        }

        *self.applied_index.lock().unwrap() = applied_index;

        // TODO: once drain_filter is stable, it could be used to make this a lot nicer
        let mut sync_requests = self.sync_requests.lock().unwrap();
        while !sync_requests.is_empty() {
            if applied_index >= sync_requests[0].0 {
                let (_, sender) = sync_requests.remove(0);
                sender.send(()).unwrap();
            } else {
                break;
            }
        }

        let messages = ready.messages.drain(..).collect();
        raft_node.advance(ready);

        Ok(messages)
    }

    fn _propose(&self, uuid: u128, data: Vec<u8>) {
        let mut raft_node = self.raft_node.lock().unwrap();
        raft_node
            .propose(uuid.to_le_bytes().to_vec(), data)
            .unwrap();
    }

    pub fn propose(
        &self,
        request: GenericRequest,
        builder: FlatBufferBuilder<'static>,
    ) -> impl Future<Item = FlatBufferBuilder<'static>, Error = ()> {
        assert!(is_write_request(request.request_type()));
        let uuid: u128 = rand::thread_rng().gen();

        self._propose(uuid, request._tab.buf.to_vec());

        // TODO: fix race. proposal could get accepted before this builder is inserted into response map
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending_responses = self.pending_responses.lock().unwrap();
            pending_responses.insert(uuid, (builder, sender));
        }

        self.process_raft_queue();

        return receiver.map_err(|_| ());
    }
}
