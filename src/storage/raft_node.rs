use log::{error, info};
use raft::eraftpb::Message;
use raft::prelude::EntryType;
use raft::storage::MemStorage;
use raft::{Config, Error, RawNode, StorageError};
use std::sync::Mutex;

use crate::generated::*;
use crate::peer_client::PeerClient;
use crate::storage::file_storage::FileStorage;
use crate::storage::lock_table::{access_type, accessed_inode, request_locks, LockTable};
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, node_id_from_address, FlatBufferResponse, ResultResponse};
use flatbuffers::FlatBufferBuilder;
use futures::channel::oneshot;
use futures::channel::oneshot::Sender;
use futures::future::{ready, Either};
use futures::FutureExt;
use futures::{Future, TryFutureExt};
use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

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

pub fn node_contains_raft_group(
    node_index: usize,
    total_nodes: usize,
    raft_group_id: u16,
    replicas_per_raft_group: usize,
) -> bool {
    assert_eq!(
        total_nodes % replicas_per_raft_group,
        0,
        "{} % {} != 0",
        total_nodes,
        replicas_per_raft_group
    );

    // Divide all nodes up into groups that can support a raft group
    let node_group = node_index / replicas_per_raft_group;
    let node_groups = total_nodes / replicas_per_raft_group;
    // Round-robin assignment of raft groups to nodes
    let assigned_node_group = raft_group_id % node_groups as u16;

    node_group == assigned_node_group as usize
}

pub struct RaftNode {
    raft_node: Mutex<RawNode<MemStorage>>,
    pending_responses: Mutex<HashMap<u128, PendingResponse>>,
    sync_requests: Mutex<Vec<(u64, Sender<()>)>>,
    leader_requests: Mutex<Vec<Sender<u64>>>,
    applied_index: AtomicU64,
    peers: HashMap<u64, PeerClient>,
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

        RaftNode {
            raft_node: Mutex::new(raft_node),
            pending_responses: Mutex::new(HashMap::new()),
            leader_requests: Mutex::new(vec![]),
            sync_requests: Mutex::new(vec![]),
            applied_index: AtomicU64::new(0),
            peers: context
                .peers
                .iter()
                .map(|peer| (node_id_from_address(peer), PeerClient::new(*peer)))
                .collect(),
            node_id,
            raft_group_id,
            file_storage: FileStorage::new(
                node_id,
                &peer_ids,
                raft_group_id,
                num_raft_groups,
                data_dir,
                &context.peers,
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
            let access_type = access_type(request.request_type());
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

        if !ready.entries().is_empty() {
            raft_node.mut_store().wl().append(ready.entries())?;
        }

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
                                error!(
                                    "Commit failed {:?} {:?}",
                                    error_code,
                                    request.request_type()
                                );
                                sender.send(Err(error_code)).ok().unwrap()
                            }
                        }
                    } else {
                        // Replicas won't have a pending response to reply to, since the node
                        // that submitted the proposal will reply to the client.
                        let builder = FlatBufferBuilder::new();
                        // TODO: pass None for builder to avoid this useless allocation
                        if let Err(err) = commit_write(request, &self.file_storage, builder) {
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
        }

        self.applied_index.store(applied_index, Ordering::SeqCst);
        // TODO: should be checkpointing to disk
        if applied_index % 100 == 0 {
            if let Err(error) = raft_node.mut_store().wl().compact(applied_index) {
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

pub fn commit_write<'a, 'b>(
    request: GenericRequest<'a>,
    file_storage: &FileStorage,
    builder: FlatBufferBuilder<'b>,
) -> Result<FlatBufferResponse<'b>, ErrorCode> {
    let response;

    match request.request_type() {
        RequestType::HardlinkIncrementRequest => {
            let hardlink_increment_request = request
                .request_as_hardlink_increment_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage
                .hardlink_stage0_link_increment(hardlink_increment_request.inode(), builder);
        }
        RequestType::HardlinkRollbackRequest => {
            let hardlink_rollback_request = request
                .request_as_hardlink_rollback_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.hardlink_rollback(
                hardlink_rollback_request.inode(),
                *hardlink_rollback_request.last_modified_time(),
                builder,
            );
        }
        RequestType::CreateInodeRequest => {
            let create_inode_request = request
                .request_as_create_inode_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.create_inode(
                create_inode_request.parent(),
                create_inode_request.uid(),
                create_inode_request.gid(),
                create_inode_request.mode(),
                create_inode_request.kind(),
                builder,
            );
        }
        RequestType::CreateLinkRequest => {
            let create_link_request = request
                .request_as_create_link_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.create_link(
                create_link_request.inode(),
                create_link_request.parent(),
                create_link_request.name(),
                *create_link_request.context(),
                create_link_request.kind(),
                builder,
            );
        }
        RequestType::ReplaceLinkRequest => {
            let replace_link_request = request
                .request_as_replace_link_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.replace_link(
                replace_link_request.parent(),
                replace_link_request.name(),
                replace_link_request.new_inode(),
                replace_link_request.kind(),
                *replace_link_request.context(),
                builder,
            );
        }
        RequestType::UpdateParentRequest => {
            let update_parent_request = request
                .request_as_update_parent_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.update_parent(
                update_parent_request.inode(),
                update_parent_request.new_parent(),
                builder,
            );
        }
        RequestType::UpdateMetadataChangedTimeRequest => {
            let update_request = request
                .request_as_update_metadata_changed_time_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.update_metadata_changed_time(update_request.inode(), builder);
        }
        RequestType::DecrementInodeRequest => {
            let decrement_inode_request = request
                .request_as_decrement_inode_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.decrement_inode_link_count(
                decrement_inode_request.inode(),
                decrement_inode_request.decrement_count(),
                builder,
            );
        }
        RequestType::RemoveLinkRequest => {
            let remove_link_request = request
                .request_as_remove_link_request()
                .ok_or(ErrorCode::BadRequest)?;
            let link_inode_and_uid = if let Some(inode) = remove_link_request.link_inode() {
                let uid = remove_link_request.link_uid().unwrap();
                Some((inode.value(), uid.value()))
            } else {
                None
            };
            response = file_storage.remove_link(
                remove_link_request.parent(),
                remove_link_request.name(),
                link_inode_and_uid,
                *remove_link_request.context(),
                builder,
            );
        }
        RequestType::LockRequest => {
            unreachable!("This should have been handled by the LockTable");
        }
        RequestType::UnlockRequest => {
            unreachable!("This should have been handled by the LockTable");
        }
        RequestType::HardlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RenameRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::ChmodRequest => {
            let chmod_request = request
                .request_as_chmod_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.chmod(
                chmod_request.inode(),
                chmod_request.mode(),
                *chmod_request.context(),
                builder,
            );
        }
        RequestType::ChownRequest => {
            let chown_request = request
                .request_as_chown_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.chown(
                chown_request.inode(),
                chown_request.uid().map(OptionalUInt::value),
                chown_request.gid().map(OptionalUInt::value),
                *chown_request.context(),
                builder,
            );
        }
        RequestType::TruncateRequest => {
            let truncate_request = request
                .request_as_truncate_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.truncate(
                truncate_request.inode(),
                truncate_request.new_length(),
                *truncate_request.context(),
                builder,
            );
        }
        RequestType::FsyncRequest => {
            let fsync_request = request
                .request_as_fsync_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.fsync(fsync_request.inode(), builder);
        }
        RequestType::CreateRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::SetXattrRequest => {
            let set_xattr_request = request
                .request_as_set_xattr_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.set_xattr(
                set_xattr_request.inode(),
                set_xattr_request.key(),
                set_xattr_request.value(),
                builder,
            );
        }
        RequestType::RemoveXattrRequest => {
            let remove_xattr_request = request
                .request_as_remove_xattr_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.remove_xattr(
                remove_xattr_request.inode(),
                remove_xattr_request.key(),
                builder,
            );
        }
        RequestType::UnlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RmdirRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::WriteRequest => {
            let write_request = request
                .request_as_write_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.write(
                write_request.inode(),
                write_request.offset(),
                write_request.data(),
                builder,
            );
        }
        RequestType::UtimensRequest => {
            let utimens_request = request
                .request_as_utimens_request()
                .ok_or(ErrorCode::BadRequest)?;
            response = file_storage.utimens(
                utimens_request.inode(),
                utimens_request.atime(),
                utimens_request.mtime(),
                *utimens_request.context(),
                builder,
            );
        }
        RequestType::MkdirRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::FilesystemCheckRequest => unreachable!(),
        RequestType::FilesystemChecksumRequest => unreachable!(),
        RequestType::LookupRequest => unreachable!(),
        RequestType::ReadRequest => unreachable!(),
        RequestType::ReadRawRequest => unreachable!(),
        RequestType::ReaddirRequest => unreachable!(),
        RequestType::GetattrRequest => unreachable!(),
        RequestType::GetXattrRequest => unreachable!(),
        RequestType::ListXattrsRequest => unreachable!(),
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::FilesystemReadyRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }

    response
}
