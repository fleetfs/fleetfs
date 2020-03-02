use crate::generated::*;
use crate::utils::FlatBufferResponse;
use flatbuffers::FlatBufferBuilder;
use futures::channel::oneshot::Sender;
use std::collections::HashMap;

pub enum FileLockType {
    // File metadata (including directory structure) is locked for writing, but reads are still allowed
    ExclusiveMetadataWriteConcurrentReadsAllowed,
}

pub enum AccessType {
    ReadData,
    ReadMetadata,
    LockMetadata,
    WriteMetadata,
    WriteDataAndMetadata,
    NoAccess,
}

type PendingResponse = (
    FlatBufferBuilder<'static>,
    Sender<Result<FlatBufferResponse<'static>, ErrorCode>>,
);

type PendingRequest = (Vec<u8>, Option<PendingResponse>);

// Locks held by the request
pub fn request_locks(request: &GenericRequest<'_>) -> Option<u64> {
    match request.request_type() {
        RequestType::RemoveLinkRequest => request
            .request_as_remove_link_request()
            .unwrap()
            .lock_id()
            .map(|x| x.value()),
        RequestType::CreateLinkRequest => request
            .request_as_create_link_request()
            .unwrap()
            .lock_id()
            .map(|x| x.value()),
        RequestType::ReplaceLinkRequest => request
            .request_as_replace_link_request()
            .unwrap()
            .lock_id()
            .map(|x| x.value()),
        RequestType::UpdateParentRequest => request
            .request_as_update_parent_request()
            .unwrap()
            .lock_id()
            .map(|x| x.value()),
        RequestType::UpdateMetadataChangedTimeRequest => request
            .request_as_update_metadata_changed_time_request()
            .unwrap()
            .lock_id()
            .map(|x| x.value()),
        RequestType::DecrementInodeRequest => request
            .request_as_decrement_inode_request()
            .unwrap()
            .lock_id()
            .map(|x| x.value()),
        RequestType::FilesystemCheckRequest
        | RequestType::FilesystemChecksumRequest
        | RequestType::ReadRequest
        | RequestType::ReadRawRequest
        | RequestType::SetXattrRequest
        | RequestType::RemoveXattrRequest
        | RequestType::UnlinkRequest
        | RequestType::RmdirRequest
        | RequestType::WriteRequest
        | RequestType::UtimensRequest
        | RequestType::ChmodRequest
        | RequestType::ChownRequest
        | RequestType::TruncateRequest
        | RequestType::FsyncRequest
        | RequestType::MkdirRequest
        | RequestType::CreateRequest
        | RequestType::LockRequest
        | RequestType::UnlockRequest
        | RequestType::HardlinkIncrementRequest
        | RequestType::HardlinkRollbackRequest
        | RequestType::CreateInodeRequest
        | RequestType::LookupRequest
        | RequestType::GetXattrRequest
        | RequestType::ListXattrsRequest
        | RequestType::ReaddirRequest
        | RequestType::GetattrRequest => None,
        RequestType::HardlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RenameRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::FilesystemReadyRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }
}

pub fn accessed_inode(request: &GenericRequest<'_>) -> Option<u64> {
    match request.request_type() {
        RequestType::FilesystemCheckRequest => None,
        RequestType::FilesystemChecksumRequest => None,
        RequestType::ReadRequest => Some(request.request_as_read_request().unwrap().inode()),
        RequestType::ReadRawRequest => Some(request.request_as_read_raw_request().unwrap().inode()),
        RequestType::SetXattrRequest => {
            Some(request.request_as_set_xattr_request().unwrap().inode())
        }
        RequestType::RemoveXattrRequest => {
            Some(request.request_as_remove_xattr_request().unwrap().inode())
        }
        RequestType::UnlinkRequest => Some(request.request_as_unlink_request().unwrap().parent()),
        RequestType::RmdirRequest => Some(request.request_as_rmdir_request().unwrap().parent()),
        RequestType::WriteRequest => Some(request.request_as_write_request().unwrap().inode()),
        RequestType::UtimensRequest => Some(request.request_as_utimens_request().unwrap().inode()),
        RequestType::ChmodRequest => Some(request.request_as_chmod_request().unwrap().inode()),
        RequestType::ChownRequest => Some(request.request_as_chown_request().unwrap().inode()),
        RequestType::TruncateRequest => {
            Some(request.request_as_truncate_request().unwrap().inode())
        }
        RequestType::FsyncRequest => None,
        RequestType::MkdirRequest => Some(request.request_as_mkdir_request().unwrap().parent()),
        RequestType::CreateRequest => Some(request.request_as_create_request().unwrap().parent()),
        RequestType::LockRequest => Some(request.request_as_lock_request().unwrap().inode()),
        RequestType::UnlockRequest => Some(request.request_as_unlock_request().unwrap().inode()),
        RequestType::HardlinkIncrementRequest => Some(
            request
                .request_as_hardlink_increment_request()
                .unwrap()
                .inode(),
        ),
        RequestType::HardlinkRollbackRequest => Some(
            request
                .request_as_hardlink_rollback_request()
                .unwrap()
                .inode(),
        ),
        RequestType::CreateInodeRequest => None,
        RequestType::DecrementInodeRequest => Some(
            request
                .request_as_decrement_inode_request()
                .unwrap()
                .inode(),
        ),
        RequestType::RemoveLinkRequest => {
            Some(request.request_as_remove_link_request().unwrap().parent())
        }
        RequestType::CreateLinkRequest => {
            Some(request.request_as_create_link_request().unwrap().parent())
        }
        RequestType::ReplaceLinkRequest => {
            Some(request.request_as_replace_link_request().unwrap().parent())
        }
        RequestType::UpdateParentRequest => {
            Some(request.request_as_update_parent_request().unwrap().inode())
        }
        RequestType::UpdateMetadataChangedTimeRequest => Some(
            request
                .request_as_update_metadata_changed_time_request()
                .unwrap()
                .inode(),
        ),
        RequestType::HardlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RenameRequest => {
            // TODO: actually make this distributed. Right now assumes that everything is stored
            None
        }
        RequestType::LookupRequest => Some(request.request_as_lookup_request().unwrap().parent()),
        RequestType::GetXattrRequest => {
            Some(request.request_as_get_xattr_request().unwrap().inode())
        }
        RequestType::ListXattrsRequest => {
            Some(request.request_as_list_xattrs_request().unwrap().inode())
        }
        RequestType::ReaddirRequest => Some(request.request_as_readdir_request().unwrap().inode()),
        RequestType::GetattrRequest => Some(request.request_as_getattr_request().unwrap().inode()),
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::FilesystemReadyRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }
}

pub fn access_type(request_type: RequestType) -> AccessType {
    match request_type {
        RequestType::HardlinkIncrementRequest => AccessType::WriteMetadata,
        RequestType::HardlinkRollbackRequest => AccessType::WriteMetadata,
        RequestType::CreateInodeRequest => AccessType::WriteMetadata,
        RequestType::CreateLinkRequest => AccessType::WriteMetadata,
        RequestType::ReplaceLinkRequest => AccessType::WriteMetadata,
        RequestType::UpdateParentRequest => AccessType::WriteMetadata,
        RequestType::UpdateMetadataChangedTimeRequest => AccessType::WriteMetadata,
        RequestType::DecrementInodeRequest => AccessType::WriteMetadata,
        RequestType::RemoveLinkRequest => AccessType::WriteMetadata,
        RequestType::LockRequest => AccessType::LockMetadata,
        RequestType::UnlockRequest => AccessType::NoAccess,
        RequestType::HardlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RenameRequest => AccessType::WriteMetadata,
        RequestType::ChmodRequest => AccessType::WriteMetadata,
        RequestType::ChownRequest => AccessType::WriteMetadata,
        RequestType::TruncateRequest => AccessType::WriteDataAndMetadata,
        RequestType::FsyncRequest => AccessType::NoAccess,
        RequestType::CreateRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::SetXattrRequest => AccessType::WriteMetadata,
        RequestType::RemoveXattrRequest => AccessType::WriteMetadata,
        RequestType::UnlinkRequest => AccessType::WriteMetadata,
        RequestType::RmdirRequest => AccessType::WriteMetadata,
        RequestType::WriteRequest => AccessType::WriteDataAndMetadata,
        RequestType::UtimensRequest => AccessType::WriteMetadata,
        RequestType::MkdirRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::FilesystemCheckRequest => unreachable!(),
        RequestType::FilesystemChecksumRequest => unreachable!(),
        RequestType::LookupRequest => AccessType::ReadMetadata,
        RequestType::ReadRequest => AccessType::ReadData,
        RequestType::ReadRawRequest => AccessType::ReadData,
        RequestType::ReaddirRequest => AccessType::ReadMetadata,
        RequestType::GetattrRequest => AccessType::ReadMetadata,
        RequestType::GetXattrRequest => AccessType::ReadMetadata,
        RequestType::ListXattrsRequest => AccessType::ReadMetadata,
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::FilesystemReadyRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }
}

// Lock table for tracking file locks
#[derive(Default)]
pub struct LockTable {
    // Requests that are waiting for the keyed inode to be unlocked
    pending_requests: HashMap<u64, Vec<PendingRequest>>,
    // Map of inodes to their lock ids, if they are locked
    lock_ids: HashMap<u64, (u64, FileLockType)>,
    next_id: u64,
}

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            pending_requests: HashMap::new(),
            lock_ids: HashMap::new(),
            next_id: 0,
        }
    }

    // XXX: For debugging, panics if the lock is held, held_lock is present, and they don't match.
    // Since it means the client thinks they have locked this inode, or is misbehaving.
    // This could happen if a lock gets revoked, so shouldn't panic in the future, but it
    // makes debugging easier for now.
    // Returns true if another client has locked this inode
    pub fn is_locked(&self, inode: u64, access_type: AccessType, held_lock: Option<u64>) -> bool {
        if let Some(id) = held_lock {
            assert_eq!(self.lock_ids[&inode].0, id);
            return false;
        }
        if !self.lock_ids.contains_key(&inode) {
            return false;
        }
        let (_, lock_type) = &self.lock_ids[&inode];
        match *lock_type {
            FileLockType::ExclusiveMetadataWriteConcurrentReadsAllowed => match access_type {
                AccessType::ReadData => {
                    unreachable!("Read requests aren't implemented for locks yet")
                }
                AccessType::ReadMetadata => {
                    unreachable!("Read requests aren't implemented for locks yet")
                }
                AccessType::LockMetadata => true,
                AccessType::WriteMetadata => true,
                AccessType::WriteDataAndMetadata => true,
                AccessType::NoAccess => false,
            },
        }
    }

    pub fn wait_for_lock(&mut self, inode: u64, request: PendingRequest) {
        assert!(self.lock_ids.contains_key(&inode));
        self.pending_requests
            .entry(inode)
            .or_default()
            .push(request);
    }

    // Returns a lock ID
    pub fn lock(&mut self, inode: u64) -> u64 {
        assert!(!self.lock_ids.contains_key(&inode));
        let lock_id = self.next_id;
        self.next_id += 1;
        self.lock_ids.insert(
            inode,
            (
                lock_id,
                FileLockType::ExclusiveMetadataWriteConcurrentReadsAllowed,
            ),
        );
        return lock_id;
    }

    // Returns a list of requests that were pending to process.
    // They may precede without re-checking whether the inode is locked.
    // Iff the last is a Lock request, a lock ID is returned
    pub fn unlock(&mut self, inode: u64, lock_id: u64) -> (Vec<PendingRequest>, Option<u64>) {
        let (existing_lock_id, _) = self.lock_ids.remove(&inode).unwrap();
        assert_eq!(existing_lock_id, lock_id);
        if !self.pending_requests.contains_key(&inode) {
            return (vec![], None);
        }

        let mut lock_id = None;
        let mut requests_to_process = self.pending_requests[&inode].len();
        for (i, (data, _)) in self.pending_requests[&inode].iter().enumerate() {
            let request = get_root_as_generic_request(data);
            if request.request_type() == RequestType::LockRequest {
                lock_id = Some(self.lock(inode));
                requests_to_process = i + 1;
                break;
            }
        }

        let requests = self
            .pending_requests
            .get_mut(&inode)
            .unwrap()
            .drain(..requests_to_process)
            .collect();
        return (requests, lock_id);
    }
}
