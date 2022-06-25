use log::{error, info, warn};
use raft::eraftpb::Message;
use raft::prelude::{ConfChange, ConfChangeType, EntryType};
use raft::storage::MemStorage;
use raft::{Config, Error, ProgressTracker, RawNode, StateRole, StorageError};
use std::sync::Mutex;

use crate::base::node_contains_raft_group;
use crate::base::node_id_from_address;
use crate::base::LocalContext;
use crate::client::{PeerClient, TcpPeerClient};
use crate::storage::local::FileStorage;
use crate::storage::lock_table::LockTable;
use crate::storage::message_handlers::commit_write;
use futures::channel::oneshot;
use futures::channel::oneshot::Sender;
use futures::future::{ready, Either};
use futures::FutureExt;
use futures::{Future, TryFutureExt};
use raft::storage::Storage;
use rand::Rng;
use slog::{o, Drain};
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::base::{ArchivedRkyvRequest, ErrorCode, RkyvGenericResponse, RkyvRequest};
use protobuf::Message as ProtobufMessage;
use rkyv::AlignedVec;

// Compact storage when it reaches 10MB
const COMPACTION_THRESHOLD: u64 = 10 * 1024 * 1024;

// Sync to ensure replicas serve latest data
pub async fn sync_with_leader(raft: &RaftNode) -> Result<(), ErrorCode> {
    let latest_commit = raft.get_latest_commit_from_leader().await?;
    raft.sync(latest_commit).await
}

type PendingResponse = Sender<Result<RkyvGenericResponse, ErrorCode>>;

fn latest_applied_on_all_followers(node_id: u64, progress: &ProgressTracker) -> u64 {
    progress
        .iter()
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
            // TODO: this is set much longer than the recommended 10x heartbeat,
            // because we don't handle leader transitions well (likely will crash)
            election_tick: 100 * 3,
            heartbeat_tick: 3,
            // TODO: need to restore this from storage
            applied: 0,
            max_size_per_msg: 1024 * 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };
        raft_config.validate().unwrap();
        let raft_storage = MemStorage::new();
        // Bridge the messages to the log backend since we use env_logger
        let slogger = slog::Logger::root(
            slog_stdlog::StdLog.fuse(),
            o!("tag" => format!("peer_{}", node_id)),
        );
        let mut raft_node = RawNode::new(&raft_config, raft_storage, &slogger).unwrap();
        // Add the peers
        // TODO: we should dynamically discover these later and process ConfChange messages in the
        // message handling loop
        for &peer in peer_ids.iter() {
            let mut conf_change = ConfChange {
                node_id: peer,
                ..Default::default()
            };
            conf_change.set_change_type(ConfChangeType::AddNode);
            let new_state = raft_node.apply_conf_change(&conf_change).unwrap();
            raft_node.mut_store().wl().set_conf_state(new_state);
        }

        let path = Path::new(&context.data_dir).join(format!("rgroup_{}", raft_group_id));
        #[allow(clippy::expect_fun_call)]
        fs::create_dir_all(&path).expect(&format!("Failed to create storage dir: {:?}", &path));

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
                &path,
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
        // TODO any way to optimize away this aligning copy?
        let mut aligned = AlignedVec::with_capacity(request_data.len());
        aligned.extend_from_slice(&request_data);
        let request = rkyv::check_archived_root::<RkyvRequest>(&aligned).unwrap();
        let request_meta = request.meta_info();

        let mut lock_table = self.lock_table.lock().unwrap();

        let mut to_process = vec![];
        if let Some(inode) = request_meta.inode {
            if lock_table.is_locked(&request_meta) {
                lock_table.wait_for_lock(inode, (request_data, pending_response));
            } else {
                match request {
                    ArchivedRkyvRequest::Lock { inode } => {
                        let lock_id = lock_table.lock(inode.into());
                        if let Some(sender) = pending_response {
                            sender
                                .send(Ok(RkyvGenericResponse::Lock { lock_id }))
                                .ok()
                                .unwrap();
                        }
                    }
                    ArchivedRkyvRequest::Unlock { inode, lock_id } => {
                        let (mut requests, new_lock_id) =
                            lock_table.unlock(inode.into(), lock_id.into());
                        if let Some(sender) = pending_response {
                            sender.send(Ok(RkyvGenericResponse::Empty)).ok().unwrap();
                        }
                        if let Some(id) = new_lock_id {
                            let (lock_request_data, pending) = requests.pop().unwrap();
                            // TODO optimize out this copy
                            let mut aligned = AlignedVec::with_capacity(lock_request_data.len());
                            aligned.extend_from_slice(&lock_request_data);
                            let lock_request =
                                rkyv::check_archived_root::<RkyvRequest>(&aligned).unwrap();
                            assert!(matches!(lock_request, ArchivedRkyvRequest::Lock { .. }));
                            if let Some(sender) = pending {
                                sender
                                    .send(Ok(RkyvGenericResponse::Lock { lock_id: id }))
                                    .ok()
                                    .unwrap();
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

        to_process
    }

    fn _process_commited_entries(
        &self,
        entries: &[raft::prelude::Entry],
        raft_node: &RawNode<MemStorage>,
    ) {
        let mut applied_index = self.applied_index.load(Ordering::SeqCst);
        for entry in entries {
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
            let to_process = self._process_lock_table(entry.data.to_vec(), pending_response);

            for (data, pending_response) in to_process {
                // TODO any way to optimize away this aligning copy?
                let mut aligned = AlignedVec::with_capacity(data.len());
                aligned.extend_from_slice(&data);
                let request = rkyv::check_archived_root::<RkyvRequest>(&aligned).unwrap();
                if let Some(sender) = pending_response {
                    match commit_write(request, &self.file_storage) {
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
                                error!("Commit failed {:?} {:?}", error_code, request);
                            }
                            sender.send(Err(error_code)).ok().unwrap()
                        }
                    }
                } else {
                    // Replicas won't have a pending response to reply to, since the node
                    // that submitted the proposal will reply to the client.
                    if let Err(error_code) = commit_write(request, &self.file_storage) {
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
                            error!("Commit failed {:?} {:?}", error_code, request);
                        }
                    }
                }

                info!(
                    "Committed write index {} (leader={}): {:?}",
                    entry.index, raft_node.raft.leader_id, request
                );
            }
        }

        self.applied_index.store(applied_index, Ordering::SeqCst);
    }

    // Returns the last applied index
    fn _process_raft_queue(&self) -> raft::Result<Vec<Message>> {
        let mut raft_node = self.raft_node.lock().unwrap();

        if !raft_node.has_ready() {
            return Ok(vec![]);
        }

        let mut ready = raft_node.ready();

        if !ready.snapshot().is_empty() {
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

        self._process_commited_entries(&ready.take_committed_entries(), &raft_node);

        // TODO: should be checkpointing to disk
        // Attempt to compact the log once it reaches the compaction threshold
        // TODO: if the log becomes too full we might OOM. Change this to write to disk
        let applied_index = self.applied_index.load(Ordering::SeqCst);
        let storage_size = self.storage_size.load(Ordering::SeqCst);
        if storage_size > COMPACTION_THRESHOLD {
            if storage_size > 2 * COMPACTION_THRESHOLD {
                warn!(
                    "Raft log storage has exceeded memory limit. Current size: {} bytes",
                    storage_size
                );
            }
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
            let last = raft_node.store().last_index().unwrap();
            let first = raft_node.store().first_index().unwrap();
            let mut new_size = 0u64;
            let entries = raft_node.store().entries(first, last, 999_999_999).unwrap();
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

        if let Err(e) = raft_node.mut_store().wl().append(ready.entries()) {
            panic!("persist raft log fail: {:?}, need to retry or panic", e);
        }

        let mut messages = ready.take_messages();
        messages.extend(ready.take_persisted_messages());
        let mut light_ready = raft_node.advance(ready);

        if let Some(commit) = light_ready.commit_index() {
            raft_node
                .mut_store()
                .wl()
                .mut_hard_state()
                .set_commit(commit);
        }
        // Send these messages as well
        messages.extend(light_ready.take_messages());
        self._process_commited_entries(&light_ready.take_committed_entries(), &raft_node);
        raft_node.advance_apply();

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
        request: &RkyvRequest,
    ) -> impl Future<Output = Result<RkyvGenericResponse, ErrorCode>> {
        let uuid: u128 = rand::thread_rng().gen();

        let (sender, receiver) = oneshot::channel();
        {
            let mut pending_responses = self.pending_responses.lock().unwrap();
            pending_responses.insert(uuid, sender);
        }
        let rkyv_bytes = rkyv::to_bytes::<_, 64>(request).unwrap();
        self._propose(uuid, rkyv_bytes.to_vec());

        self.process_raft_queue();

        receiver.map(|x| x.unwrap_or(Err(ErrorCode::Uncategorized)))
    }

    pub fn propose_raw(
        &self,
        request: AlignedVec,
    ) -> impl Future<Output = Result<RkyvGenericResponse, ErrorCode>> {
        let uuid: u128 = rand::thread_rng().gen();

        let (sender, receiver) = oneshot::channel();
        {
            let mut pending_responses = self.pending_responses.lock().unwrap();
            pending_responses.insert(uuid, sender);
        }
        self._propose(uuid, request.to_vec());

        self.process_raft_queue();

        receiver.map(|x| x.unwrap_or(Err(ErrorCode::Uncategorized)))
    }
}
