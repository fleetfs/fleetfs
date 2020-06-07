use crate::base::AccessType;
use crate::base::FlatBufferResponse;
use crate::generated::*;
use flatbuffers::FlatBufferBuilder;
use futures::channel::oneshot::Sender;
use std::collections::HashMap;

pub enum FileLockType {
    // File metadata (including directory structure) is locked for writing, but reads are still allowed
    ExclusiveMetadataWriteConcurrentReadsAllowed,
}

type PendingResponse = (
    FlatBufferBuilder<'static>,
    Sender<Result<FlatBufferResponse<'static>, ErrorCode>>,
);

type PendingRequest = (Vec<u8>, Option<PendingResponse>);

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
