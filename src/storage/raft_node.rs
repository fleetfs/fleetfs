use log::{error, info};
use raft::eraftpb::Message;
use raft::prelude::EntryType;
use raft::storage::MemStorage;
use raft::{Config, Error, ProgressSet, RawNode, StateRole, StorageError};
use std::sync::Mutex;

use crate::base::node_contains_raft_group;
use crate::base::LocalContext;
use crate::base::{access_type, accessed_inode, request_locks};
use crate::base::{empty_response, node_id_from_address, FlatBufferResponse, ResultResponse};
use crate::client::{PeerClient, TcpPeerClient};
use crate::generated::*;
use crate::storage::local::FileStorage;
use crate::storage::lock_table::LockTable;
use crate::storage::message_handlers::commit_write;
use flatbuffers::FlatBufferBuilder;
use futures::channel::oneshot;
use futures::channel::oneshot::Sender;
use futures::future::{ready, Either};
use futures::FutureExt;
use futures::{Future, TryFutureExt};
use raft::storage::Storage;
use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use protobuf::Message as ProtobufMessage;

// Compact storage when it reaches 10MB
const COMPACTION_THRESHOLD: u64 = 10 * 1024 * 1024;

// Sync to ensure replicas serve latest data
pub async fn sync_with_leader(raft: &RaftNode) -> Result<(), ErrorCode> {
    let latest_commit = raft.get_latest_commit_from_leader().await?;
    raft.sync(latest_commit).await
}

fn lock_response(mut buffer: FlatBufferBuilder, lock_id: u64) -> ResultResponse {
    let mut response_builder = LockResponseBuilder::new(&mut buffer);
    response_builder.add_lock_id(lock_id);
    let offset = response_builder.finish().as_union_value();
    return Ok((buffer, ResponseType::LockResponse, offset));
}

type PendingResponse = (
    FlatBufferBuilder<'static>,
    Sender<Result<FlatBufferResponse<'static>, ErrorCode>>,
);

fn latest_applied_on_all_followers(node_id: u64, progress_set: &ProgressSet) -> u64 {
    progress_set
        .voters()
        .filter(|(id, _)| **id != node_id)
        .map(|(_, progress)| progress.matched)
        .min()
        .unwrap()
}

pub struct RaftNode {
    raft_node: Mutex<RawNode<MemStorage>>,
    pending_responses: Mutex<HashMap<u128, PendingResponse>>,
    sync_requests: Mutex<Vec<(u64, Sender<()>)>>,
    leader_requests: Mutex<Vec<Sender<u64>>>,
    applied_index: AtomicU64,
    // TODO: this should be part of MemStorage, once we implement our own raft log storage
    storage_size: AtomicU64,
    peers: HashMap<u64, TcpPeerClient>,
    raft_group_id: u16,
    node_id: u64,
    file_storage: FileStorage,
    lock_table: Mutex<LockTable>,
}

impl RaftNode {
    pub fn new(context: LocalContext, raft_group_id: u16, num_raft_groups: u16) -> RaftNode {
        // TODO: currently all rgroups reuse the same set of node_ids. Debugging would be easier,
        // if they had unique ids
        let node_id = context.node_id;
        let mut peer_ids: Vec<u64> = context
            .peers_with_node_indices()
            .iter()
            .map(|(peer, peer_index)| (node_id_from_address(peer), *peer_index))
            .filter(|(_, peer_index)| {
                node_contains_raft_group(
                    *peer_index,
                    context.total_nodes(),
                    raft_group_id,
                    context.replicas_per_raft_group,
                )
            })
            .map(|(peer_id, _)| peer_id)
            .collect();
        assert!(node_contains_raft_group(
            context.node_index(),
            context.total_nodes(),
            raft_group_id,
            context.replicas_per_raft_group,
        ));
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
            tag: format!("peer_{}", node_id),
            ..Default::default()
        };
        let raft_storage = MemStorage::new();
        let raft_node = RawNode::new(&raft_config, raft_storage, vec![]).unwrap();

        let path = Path::new(&context.data_dir).join(format!("rgroup_{}", raft_group_id));
        #[allow(clippy::expect_fun_call)]
        fs::create_dir_all(&path).expect(&format!("Failed to create data dir: {:?}", &path));
        let data_dir = path.to_str().unwrap();

        let peer_addresses: Vec<SocketAddr> = context
            .peers
            .iter()
            .filter(|peer| peer_ids.contains(&node_id_from_address(peer)))
            .cloned()
            .collect();

        RaftNode {
            raft_node: Mutex::new(raft_node),
            pending_responses: Mutex::new(HashMap::new()),
            leader_requests: Mutex::new(vec![]),
            sync_requests: Mutex::new(vec![]),
            applied_index: AtomicU64::new(0),
            storage_size: AtomicU64::new(0),
            peers: peer_addresses
                .iter()
                .map(|peer| (node_id_from_address(peer), TcpPeerClient::new(*peer)))
                .collect(),
            node_id,
            raft_group_id,
            file_storage: FileStorage::new(
                node_id,
                raft_group_id,
                num_raft_groups,
                data_dir,
                &peer_addresses,
            ),
            lock_table: Mutex::new(LockTable::new()),
        }
    }

    pub fn get_raft_group_id(&self) -> u16 {
        self.raft_group_id
    }

    pub fn local_data_checksum(&self) -> Result<Vec<u8>, ErrorCode> {
        self.file_storage.local_data_checksum()
    }

    // TODO: remove this method
    pub fn file_storage(&self) -> &FileStorage {
        &self.file_storage
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
            tokio::spawn(peer.send_raft_message(self.raft_group_id, message));
        }
    }

    pub fn get_latest_local_commit(&self) -> u64 {
        let raft_node = self.raft_node.lock().unwrap();

        if raft_node.raft.leader_id == self.node_id {
            return self.applied_index.load(Ordering::SeqCst);
        }

        unreachable!();
    }

    // TODO: we need to also get the term from the leader. The index alone isn't meaningful
    pub fn get_latest_commit_from_leader(&self) -> impl Future<Output = Result<u64, ErrorCode>> {
        let raft_node = self.raft_node.lock().unwrap();

        if raft_node.raft.leader_id == self.node_id {
            Either::Left(ready(Ok(self.applied_index.load(Ordering::SeqCst))))
        } else if raft_node.raft.leader_id == 0 {
            // TODO: wait for a leader
            Either::Left(ready(Ok(0)))
        } else {
            let leader = &self.peers[&raft_node.raft.leader_id];
            Either::Right(
                leader
                    .get_latest_commit(self.raft_group_id)
                    .map_err(|_| ErrorCode::Uncategorized),
            )
        }
    }

    pub fn get_leader(&self) -> impl Future<Output = Result<u64, ErrorCode>> {
        let raft_node = self.raft_node.lock().unwrap();

        if raft_node.raft.leader_id > 0 {
            Either::Left(ready(Ok(raft_node.raft.leader_id)))
        } else {
            let (sender, receiver) = oneshot::channel();
            self.leader_requests.lock().unwrap().push(sender);
            Either::Right(receiver.map(|x| x.map_err(|_| ErrorCode::Uncategorized)))
        }
    }

    // Wait until the given index has been committed
    pub fn sync(&self, index: u64) -> impl Future<Output = Result<(), ErrorCode>> {
        // Make sure we have the lock on all data structures
        let _raft_node_locked = self.raft_node.lock().unwrap();

        if self.applied_index.load(Ordering::SeqCst) >= index {
            Either::Left(ready(Ok(())))
        } else {
            let (sender, receiver) = oneshot::channel();
            self.sync_requests.lock().unwrap().push((index, sender));
            Either::Right(receiver.map(|x| x.map_err(|_| ErrorCode::Uncategorized)))
        }
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

    fn _process_lock_table(
        &self,
        request_data: Vec<u8>,
        pending_response: Option<PendingResponse>,
    ) -> Vec<(Vec<u8>, Option<PendingResponse>)> {
        let request = get_root_as_generic_request(&request_data);
        let mut lock_table = self.lock_table.lock().unwrap();

        let mut to_process = vec![];
        if let Some(inode) = accessed_inode(&request) {
            let held_lock = request_locks(&request);
            let access_type = access_type(&request);
            if lock_table.is_locked(inode, access_type, held_lock) {
                lock_table.wait_for_lock(inode, (request_data, pending_response));
            } else {
                match request.request_type() {
                    RequestType::LockRequest => {
                        let lock_id = lock_table.lock(inode);
                        if let Some((builder, sender)) = pending_response {
                            sender.send(lock_response(builder, lock_id)).ok().unwrap();
                        }
                    }
                    RequestType::UnlockRequest => {
                        let held_lock_id = request.request_as_unlock_request().unwrap().lock_id();
                        let (mut requests, new_lock_id) = lock_table.unlock(inode, held_lock_id);
                        if let Some((builder, sender)) = pending_response {
                            sender.send(empty_response(builder)).ok().unwrap();
                        }
                        if let Some(id) = new_lock_id {
                            let (lock_request_data, pending) = requests.pop().unwrap();
                            let lock_request = get_root_as_generic_request(&lock_request_data);
                            assert_eq!(lock_request.request_type(), RequestType::LockRequest);
                            if let Some((builder, sender)) = pending {
                                sender.send(lock_response(builder, id)).ok().unwrap();
                            }
                        }
                        to_process.extend(requests);
                    }
                    _ => {
                        // Default to processing the request
                        to_process.push((request_data, pending_response));
                    }
                }
            }
        } else {
            // If it doesn't access an inode, then just process the request
            to_process.push((request_data, pending_response));
        }

        return to_process;
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

        let mut entries_size = 0u64;
        if !ready.entries().is_empty() {
            for entry in ready.entries().iter() {
                entries_size += entry.compute_size() as u64;
            }
            raft_node.mut_store().wl().append(ready.entries())?;
        }
        self.storage_size.fetch_add(entries_size, Ordering::SeqCst);

        if let Some(hard_state) = ready.hs() {
            raft_node.mut_store().wl().set_hardstate(hard_state.clone());
        }

        let mut applied_index = self.applied_index.load(Ordering::SeqCst);
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

                let mut uuid = [0; 16];
                uuid.copy_from_slice(&entry.context[0..16]);
                let pending_response = pending_responses.remove(&u128::from_le_bytes(uuid));
                let to_process = self._process_lock_table(entry.data, pending_response);

                for (data, pending_response) in to_process {
                    let request = get_root_as_generic_request(&data);
                    if let Some((builder, sender)) = pending_response {
                        match commit_write(request, &self.file_storage, builder) {
                            Ok(response) => sender.send(Ok(response)).ok().unwrap(),
                            // TODO: handle this somehow. If not all nodes failed, then the filesystem
                            // is probably corrupted, since some will have applied the write, but not all
                            // There should only be a few types of messages that can fail here. truncate is one,
                            // since you can call it with LONG_MAX or some other value that balloons
                            // the message into a huge write. Probably most other messages can't fail
                            Err(error_code) => {
                                // Ignore errors which the user caused
                                if error_code != ErrorCode::InodeDoesNotExist
                                    && error_code != ErrorCode::DoesNotExist
                                    && error_code != ErrorCode::AccessDenied
                                    && error_code != ErrorCode::OperationNotPermitted
                                    && error_code != ErrorCode::AlreadyExists
                                    && error_code != ErrorCode::NotEmpty
                                    && error_code != ErrorCode::InvalidXattrNamespace
                                    && error_code != ErrorCode::MissingXattrKey
                                {
                                    error!(
                                        "Commit failed {:?} {:?}",
                                        error_code,
                                        request.request_type()
                                    );
                                }
                                sender.send(Err(error_code)).ok().unwrap()
                            }
                        }
                    } else {
                        // Replicas won't have a pending response to reply to, since the node
                        // that submitted the proposal will reply to the client.
                        let builder = FlatBufferBuilder::new();
                        // TODO: pass None for builder to avoid this useless allocation
                        if let Err(error_code) = commit_write(request, &self.file_storage, builder)
                        {
                            // TODO: handle this somehow. If not all nodes failed, then the filesystem
                            // is probably corrupted, since some will have applied the write, but not all.
                            // There should only be a few types of messages that can fail here. truncate is one,
                            // since you can call it with LONG_MAX or some other value that balloons
                            // the message into a huge write. Probably most other messages can't fail

                            // Ignore errors which the user caused
                            if error_code != ErrorCode::InodeDoesNotExist
                                && error_code != ErrorCode::DoesNotExist
                                && error_code != ErrorCode::AccessDenied
                                && error_code != ErrorCode::OperationNotPermitted
                                && error_code != ErrorCode::AlreadyExists
                                && error_code != ErrorCode::NotEmpty
                                && error_code != ErrorCode::InvalidXattrNamespace
                                && error_code != ErrorCode::MissingXattrKey
                            {
                                error!(
                                    "Commit failed {:?} {:?}",
                                    error_code,
                                    request.request_type()
                                );
                            }
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
        }

        self.applied_index.store(applied_index, Ordering::SeqCst);
        // TODO: should be checkpointing to disk
        // Attempt to compact the log once it reaches the compaction threshold
        // TODO: if the log becomes too full we might OOM. Change this to write to disk
        if self.storage_size.load(Ordering::SeqCst) > COMPACTION_THRESHOLD {
            // TODO: Snapshots aren't implemented, so ensure that we keep any entries
            // that still need to be replicated to a follower(s)
            let mut compact_to = match raft_node.raft.state {
                StateRole::Leader => {
                    latest_applied_on_all_followers(self.node_id, raft_node.raft.prs())
                }
                StateRole::Follower => applied_index,
                StateRole::Candidate | StateRole::PreCandidate => applied_index,
            };
            if compact_to > 0 {
                compact_to -= 1;
            }
            if let Err(error) = raft_node.mut_store().wl().compact(compact_to) {
                match error {
                    Error::Store(store_error) => match store_error {
                        StorageError::Compacted => {} // no-op
                        e => {
                            panic!("Error during compaction: {}", e);
                        }
                    },
                    e => {
                        panic!("Error during compaction: {}", e);
                    }
                }
            }
            let last = raft_node.get_store().last_index().unwrap();
            let first = raft_node.get_store().first_index().unwrap();
            let mut new_size = 0u64;
            let entries = raft_node
                .get_store()
                .entries(first, last, 999_999_999)
                .unwrap();
            for entry in entries {
                new_size += entry.compute_size() as u64;
            }
            self.storage_size.store(new_size, Ordering::SeqCst);
        }

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
    ) -> impl Future<Output = Result<FlatBufferResponse<'static>, ErrorCode>> {
        let uuid: u128 = rand::thread_rng().gen();

        let (sender, receiver) = oneshot::channel();
        {
            let mut pending_responses = self.pending_responses.lock().unwrap();
            pending_responses.insert(uuid, (builder, sender));
        }
        // TODO: is accessing _tab.buf safe?
        self._propose(uuid, request._tab.buf.to_vec());

        self.process_raft_queue();

        return receiver.map(|x| x.unwrap_or_else(|_| Err(ErrorCode::Uncategorized)));
    }
}
