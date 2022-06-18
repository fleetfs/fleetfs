use crate::base::{
    flatbuffer_request_meta_info, AccessType, DistributionRequirement, RequestMetaInfo,
};
use crate::generated::*;
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Add;
use std::time::{Duration, SystemTime};

#[derive(Archive, Debug, Deserialize, PartialEq, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub enum ErrorCode {
    DoesNotExist,
    InodeDoesNotExist,
    FileTooLarge,
    AccessDenied,
    OperationNotPermitted,
    AlreadyExists,
    NameTooLong,
    NotEmpty,
    MissingXattrKey,
    BadResponse,
    BadRequest,
    Corrupted,
    RaftFailure,
    InvalidXattrNamespace,
    Uncategorized,
}

#[derive(Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub enum RkyvRequest {
    FilesystemReady,
    FilesystemInformation,
    FilesystemChecksum,
    FilesystemCheck,
    Fsync {
        inode: u64,
    },
    GetAttr {
        inode: u64,
    },
    ListDir {
        inode: u64,
    },
    ListXattrs {
        inode: u64,
    },
    LatestCommit {
        raft_group: u16,
    },
    RaftGroupLeader {
        raft_group: u16,
    },
    RaftMessage {
        raft_group: u16,
        data: Vec<u8>,
    },
    Flatbuffer(Vec<u8>),
    // Internal request to lock an inode
    Lock {
        inode: u64,
    },
    // Internal request to unlock an inode
    Unlock {
        inode: u64,
        lock_id: u64,
    },

    // Internal transaction messages

    // Used internally to rollback hardlink transactions
    HardlinkRollback {
        inode: u64,
        last_modified_time: Timestamp,
    },
}

#[derive(Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub struct DirectoryEntry {
    pub inode: u64,
    pub name: String,
    pub kind: u8,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, Copy)]
#[archive_attr(derive(CheckBytes))]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: i32,
}

impl Timestamp {
    pub fn new(seconds: i64, nanos: i32) -> Self {
        Self { seconds, nanos }
    }
}

impl Into<SystemTime> for Timestamp {
    fn into(self) -> SystemTime {
        SystemTime::UNIX_EPOCH.add(Duration::new(self.seconds as u64, self.nanos as u32))
    }
}

impl Into<Timestamp> for ArchivedTimestamp {
    fn into(self) -> Timestamp {
        Timestamp {
            seconds: self.seconds.into(),
            nanos: self.nanos.into(),
        }
    }
}

impl Into<Timestamp> for &ArchivedTimestamp {
    fn into(self) -> Timestamp {
        Timestamp {
            seconds: self.seconds.into(),
            nanos: self.nanos.into(),
        }
    }
}

#[derive(Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub struct EntryMetadata {
    pub inode: u64,
    pub size_bytes: u64,
    pub size_blocks: u64,
    pub last_access_time: Timestamp,
    pub last_modified_time: Timestamp,
    pub last_metadata_modified_time: Timestamp,
    pub kind: u8,
    pub mode: u16,
    pub hard_links: u32,
    pub user_id: u32,
    pub group_id: u32,
    pub device_id: u32,
    pub block_size: u32,
    // The number of directory entries in the directory. Only available if kind == Directory
    pub directory_entries: Option<u32>,
}

#[derive(Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub enum RkyvGenericResponse {
    Lock {
        lock_id: u64,
    },
    FilesystemInformation {
        block_size: u32,
        max_name_length: u32,
    },
    NodeId {
        id: u64,
    },
    Inode {
        id: u64,
    },
    RemovedInode {
        id: u64,
        complete: bool,
    },
    Written {
        bytes_written: u32,
    },
    Xattrs {
        attrs: Vec<String>,
    },
    Read {
        data: Vec<u8>,
    },
    LatestCommit {
        term: u64,
        index: u64,
    },
    DirectoryListing(Vec<DirectoryEntry>),
    EntryMetadata(EntryMetadata),
    HardlinkTransaction {
        rollback_last_modified: Timestamp,
        attrs: EntryMetadata,
    },
    Empty,
    ErrorOccurred(ErrorCode),
    // Mapping from raft group ids to their checksum
    Checksums(HashMap<u16, Vec<u8>>),
}

impl Debug for ArchivedRkyvRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchivedRkyvRequest::FilesystemReady => write!(f, "FilesystemReady"),
            ArchivedRkyvRequest::FilesystemInformation => write!(f, "FilesystemInformation"),
            ArchivedRkyvRequest::FilesystemChecksum => write!(f, "FilesystemChecksum"),
            ArchivedRkyvRequest::FilesystemCheck => write!(f, "FilesystemCheck"),
            ArchivedRkyvRequest::Fsync { inode } => write!(f, "Fsync: {}", inode),
            ArchivedRkyvRequest::GetAttr { inode } => write!(f, "GetAttr: {}", inode),
            ArchivedRkyvRequest::ListDir { inode } => write!(f, "ListDir: {}", inode),
            ArchivedRkyvRequest::ListXattrs { inode } => write!(f, "ListXattrs: {}", inode),
            ArchivedRkyvRequest::LatestCommit { raft_group } => {
                write!(f, "LatestCommit: {}", raft_group)
            }
            ArchivedRkyvRequest::RaftGroupLeader { raft_group } => {
                write!(f, "RaftGroupLeader: {}", raft_group)
            }
            ArchivedRkyvRequest::RaftMessage { raft_group, .. } => {
                write!(f, "RaftMessage: {}", raft_group)
            }
            ArchivedRkyvRequest::Flatbuffer(_) => write!(f, "Flatbuffer"),
            ArchivedRkyvRequest::Lock { inode } => write!(f, "Lock: {}", inode),
            ArchivedRkyvRequest::Unlock { inode, lock_id } => {
                write!(f, "Unlock: {}, {}", inode, lock_id)
            }
            ArchivedRkyvRequest::HardlinkRollback { inode, .. } => {
                write!(f, "HardlinkRollback: {}", inode)
            }
        }
    }
}

impl ArchivedRkyvRequest {
    pub fn meta_info(&self) -> RequestMetaInfo {
        match self {
            ArchivedRkyvRequest::FilesystemReady
            | ArchivedRkyvRequest::FilesystemInformation
            | ArchivedRkyvRequest::FilesystemCheck
            | ArchivedRkyvRequest::FilesystemChecksum => RequestMetaInfo {
                raft_group: None,
                inode: None,
                lock_id: None,
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::Any,
            },
            ArchivedRkyvRequest::Fsync { inode } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::ListXattrs { inode }
            | ArchivedRkyvRequest::ListDir { inode }
            | ArchivedRkyvRequest::GetAttr { inode } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::ReadMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::LatestCommit { raft_group }
            | ArchivedRkyvRequest::RaftGroupLeader { raft_group }
            | ArchivedRkyvRequest::RaftMessage { raft_group, .. } => RequestMetaInfo {
                raft_group: Some(raft_group.into()),
                inode: None,
                lock_id: None,
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::Lock { inode } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::LockMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::Unlock { inode, lock_id } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: Some(lock_id.into()),
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::HardlinkRollback { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::Flatbuffer(data) => {
                let request = get_root_as_generic_request(data);
                flatbuffer_request_meta_info(&request)
            }
        }
    }
}

// Add some helper methods to the generated rkyv type for RkyvGenericResponse
impl ArchivedRkyvGenericResponse {
    pub fn as_checksum_response(&self) -> Option<HashMap<u16, Vec<u8>>> {
        if let ArchivedRkyvGenericResponse::Checksums(checksums) = self {
            let mut result = HashMap::new();
            for (key, value) in checksums.iter() {
                result.insert(key.into(), value.as_slice().to_vec());
            }
            Some(result)
        } else {
            None
        }
    }

    pub fn as_attr_response(&self) -> Option<EntryMetadata> {
        if let ArchivedRkyvGenericResponse::EntryMetadata(attr) = self {
            let entry: EntryMetadata = attr.deserialize(&mut rkyv::Infallible).unwrap();
            Some(entry)
        } else {
            None
        }
    }

    pub fn as_directory_listing_response(&self) -> Option<Vec<DirectoryEntry>> {
        if let ArchivedRkyvGenericResponse::DirectoryListing(entries) = self {
            let mut result = vec![];
            for entry in entries.iter() {
                result.push(DirectoryEntry {
                    inode: entry.inode.into(),
                    name: entry.name.to_string(),
                    kind: entry.kind,
                });
            }
            Some(result)
        } else {
            None
        }
    }

    pub fn as_error_response(&self) -> Option<ErrorCode> {
        if let ArchivedRkyvGenericResponse::ErrorOccurred(archived) = self {
            let error_code = match archived {
                ArchivedErrorCode::DoesNotExist => ErrorCode::DoesNotExist,
                ArchivedErrorCode::InodeDoesNotExist => ErrorCode::InodeDoesNotExist,
                ArchivedErrorCode::FileTooLarge => ErrorCode::FileTooLarge,
                ArchivedErrorCode::AccessDenied => ErrorCode::AccessDenied,
                ArchivedErrorCode::OperationNotPermitted => ErrorCode::OperationNotPermitted,
                ArchivedErrorCode::AlreadyExists => ErrorCode::AlreadyExists,
                ArchivedErrorCode::NameTooLong => ErrorCode::NameTooLong,
                ArchivedErrorCode::NotEmpty => ErrorCode::NotEmpty,
                ArchivedErrorCode::MissingXattrKey => ErrorCode::MissingXattrKey,
                ArchivedErrorCode::BadResponse => ErrorCode::BadResponse,
                ArchivedErrorCode::BadRequest => ErrorCode::BadRequest,
                ArchivedErrorCode::Corrupted => ErrorCode::Corrupted,
                ArchivedErrorCode::RaftFailure => ErrorCode::RaftFailure,
                ArchivedErrorCode::InvalidXattrNamespace => ErrorCode::InvalidXattrNamespace,
                ArchivedErrorCode::Uncategorized => ErrorCode::Uncategorized,
            };
            Some(error_code)
        } else {
            None
        }
    }

    pub fn as_empty_response(&self) -> Option<()> {
        if matches!(self, ArchivedRkyvGenericResponse::Empty) {
            Some(())
        } else {
            None
        }
    }

    pub fn as_read_response(&self) -> Option<&[u8]> {
        if let ArchivedRkyvGenericResponse::Read { data } = self {
            Some(data.as_slice())
        } else {
            None
        }
    }

    pub fn as_xattrs_response(&self) -> Option<Vec<&str>> {
        if let ArchivedRkyvGenericResponse::Xattrs { attrs } = self {
            Some(attrs.iter().map(|x| x.as_str()).collect())
        } else {
            None
        }
    }

    pub fn as_bytes_written_response(&self) -> Option<u32> {
        if let ArchivedRkyvGenericResponse::Written { bytes_written } = self {
            Some(bytes_written.into())
        } else {
            None
        }
    }

    pub fn as_inode_response(&self) -> Option<u64> {
        if let ArchivedRkyvGenericResponse::Inode { id } = self {
            Some(id.into())
        } else {
            None
        }
    }

    pub fn as_latest_commit_response(&self) -> Option<(u64, u64)> {
        if let ArchivedRkyvGenericResponse::LatestCommit { term, index } = self {
            Some((term.into(), index.into()))
        } else {
            None
        }
    }
}
