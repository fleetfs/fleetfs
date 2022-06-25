use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Add;
use std::time::{Duration, SystemTime};

pub struct RequestMetaInfo {
    pub raft_group: Option<u16>,
    pub inode: Option<u64>, // Some if the request accesses a single inode (i.e. None for Rename)
    pub lock_id: Option<u64>,
    pub access_type: AccessType, // Used to determine locks to acquire
    pub distribution_requirement: DistributionRequirement,
}

pub enum AccessType {
    ReadData,
    ReadMetadata,
    LockMetadata,
    WriteMetadata,
    WriteDataAndMetadata,
    NoAccess,
}

// Where this message can be processed
pub enum DistributionRequirement {
    Any,                    // Any node can process this message
    TransactionCoordinator, // Any node can process this message by acting as a transcation coordinator
    RaftGroup,              // Must be processed by a specific rgroup
    Node,                   // Must be processed by a specific node
}

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

#[derive(Archive, Debug, Deserialize, PartialEq, Serialize, Clone, Copy)]
#[archive_attr(derive(CheckBytes))]
pub enum FileKind {
    File,
    Directory,
    Symlink,
}

impl From<&ArchivedFileKind> for FileKind {
    fn from(archived: &ArchivedFileKind) -> Self {
        match archived {
            ArchivedFileKind::File => FileKind::File,
            ArchivedFileKind::Directory => FileKind::Directory,
            ArchivedFileKind::Symlink => FileKind::Symlink,
        }
    }
}

#[derive(Archive, Debug, Deserialize, PartialEq, Serialize, Clone, Copy)]
#[archive_attr(derive(CheckBytes))]
pub struct CommitId {
    pub term: u64,
    pub index: u64,
}

impl CommitId {
    pub fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}

#[derive(Archive, Debug, Deserialize, PartialEq, Serialize, Clone, Copy)]
#[archive_attr(derive(CheckBytes))]
pub struct InodeUidPair {
    pub inode: u64,
    pub uid: u32,
}

impl InodeUidPair {
    pub fn new(inode: u64, uid: u32) -> Self {
        Self { inode, uid }
    }
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
    Create {
        parent: u64,
        name: String,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    },
    Mkdir {
        parent: u64,
        name: String,
        uid: u32,
        gid: u32,
        mode: u16,
    },
    Unlink {
        parent: u64,
        name: String,
        context: UserContext,
    },
    Rmdir {
        parent: u64,
        name: String,
        context: UserContext,
    },
    Truncate {
        inode: u64,
        new_length: u64,
        context: UserContext,
    },
    Rename {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
        context: UserContext,
    },
    Lookup {
        parent: u64,
        name: String,
        context: UserContext,
    },
    Chown {
        inode: u64,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
    },
    Chmod {
        inode: u64,
        mode: u32,
        context: UserContext,
    },
    Utimens {
        inode: u64,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        context: UserContext,
    },
    Hardlink {
        inode: u64,
        new_parent: u64,
        new_name: String,
        context: UserContext,
    },
    ListXattrs {
        inode: u64,
    },
    GetXattr {
        inode: u64,
        key: String,
        context: UserContext,
    },
    SetXattr {
        inode: u64,
        key: String,
        value: Vec<u8>,
        context: UserContext,
    },
    RemoveXattr {
        inode: u64,
        key: String,
        context: UserContext,
    },
    // Reads only the blocks of data on this node
    ReadRaw {
        required_commit: CommitId,
        inode: u64,
        offset: u64,
        read_size: u32,
    },
    Read {
        inode: u64,
        offset: u64,
        read_size: u32,
    },
    Write {
        inode: u64,
        offset: u64,
        data: Vec<u8>,
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
    // Internal request to create directory link as part of a transaction. Does not increment the inode's link count.
    CreateLink {
        parent: u64,
        name: String,
        inode: u64,
        kind: FileKind,
        lock_id: Option<u64>,
        context: UserContext,
    },
    // Internal request to atomically replace a directory link, so that it points to a different inode,
    // as part of a transaction. Does not change either inode's link count. It is the callers responsibility to ensure
    // that replacing the existing link is safe (i.e. it doesn't point to a non-empty directory)
    ReplaceLink {
        parent: u64,
        name: String,
        new_inode: u64,
        kind: FileKind,
        lock_id: Option<u64>,
        context: UserContext,
    },
    // Used internally to remove a link entry from a directory. Does *not* decrement the hard link count of the target inode
    RemoveLink {
        parent: u64,
        name: String,
        link_inode_and_uid: Option<InodeUidPair>,
        lock_id: Option<u64>,
        context: UserContext,
    },
    // Internal request to create an inode as part of a create() or mkdir() transaction
    CreateInode {
        raft_group: u16,
        parent: u64,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    },
    // Used internally for stage0 of hardlink transactions
    HardlinkIncrement {
        inode: u64,
    },
    // Internal request to update the parent link of a directory inode
    UpdateParent {
        inode: u64,
        new_parent: u64,
        lock_id: Option<u64>,
    },
    // Internal request to update the metadata changed time an inode
    UpdateMetadataChangedTime {
        inode: u64,
        lock_id: Option<u64>,
    },
    // TODO: raft messages have to be idempotent. This one is not.
    // Internal request to decrement inode link count. Will delete the inode if its count reaches zero.
    DecrementInode {
        inode: u64,
        // The number of times to decrement the link count
        decrement_count: u32,
        lock_id: Option<u64>,
    },
}

#[derive(Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub struct DirectoryEntry {
    pub inode: u64,
    pub name: String,
    pub kind: FileKind,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, Copy)]
#[archive_attr(derive(CheckBytes))]
pub struct UserContext {
    pub uid: u32,
    pub gid: u32,
}

impl UserContext {
    pub fn new(uid: u32, gid: u32) -> Self {
        Self { uid, gid }
    }

    pub fn uid(&self) -> u32 {
        self.uid
    }

    pub fn gid(&self) -> u32 {
        self.gid
    }
}

impl From<&ArchivedUserContext> for UserContext {
    fn from(context: &ArchivedUserContext) -> Self {
        Self {
            uid: context.uid.into(),
            gid: context.gid.into(),
        }
    }
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

impl From<Timestamp> for SystemTime {
    fn from(timestamp: Timestamp) -> Self {
        SystemTime::UNIX_EPOCH.add(Duration::new(
            timestamp.seconds as u64,
            timestamp.nanos as u32,
        ))
    }
}

impl From<&ArchivedTimestamp> for Timestamp {
    fn from(archived: &ArchivedTimestamp) -> Self {
        Timestamp {
            seconds: archived.seconds.into(),
            nanos: archived.nanos.into(),
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
    pub kind: FileKind,
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
            ArchivedRkyvRequest::Create { .. } => write!(f, "Create"),
            ArchivedRkyvRequest::Mkdir { .. } => write!(f, "Mkdir"),
            ArchivedRkyvRequest::Unlink { .. } => write!(f, "Unlink"),
            ArchivedRkyvRequest::Truncate { .. } => write!(f, "Truncate"),
            ArchivedRkyvRequest::Rmdir { .. } => write!(f, "Rmdir"),
            ArchivedRkyvRequest::Rename { .. } => write!(f, "Rename"),
            ArchivedRkyvRequest::Lookup { .. } => write!(f, "Lookup"),
            ArchivedRkyvRequest::Chown { .. } => write!(f, "Chown"),
            ArchivedRkyvRequest::Chmod { .. } => write!(f, "Chmod"),
            ArchivedRkyvRequest::Utimens { .. } => write!(f, "Utimens"),
            ArchivedRkyvRequest::Hardlink { .. } => write!(f, "Hardlink"),
            ArchivedRkyvRequest::CreateLink { .. } => write!(f, "CreateLink"),
            ArchivedRkyvRequest::ReplaceLink { .. } => write!(f, "ReplaceLink"),
            ArchivedRkyvRequest::RemoveLink { .. } => write!(f, "RemoveLink"),
            ArchivedRkyvRequest::CreateInode { .. } => write!(f, "CreateInode"),
            ArchivedRkyvRequest::Fsync { inode } => write!(f, "Fsync: {}", inode),
            ArchivedRkyvRequest::GetAttr { inode } => write!(f, "GetAttr: {}", inode),
            ArchivedRkyvRequest::ListDir { inode } => write!(f, "ListDir: {}", inode),
            ArchivedRkyvRequest::ListXattrs { inode } => write!(f, "ListXattrs: {}", inode),
            ArchivedRkyvRequest::GetXattr { .. } => write!(f, "GetXattr"),
            ArchivedRkyvRequest::SetXattr { .. } => write!(f, "SetXattr"),
            ArchivedRkyvRequest::RemoveXattr { .. } => write!(f, "RemoveXattr"),
            ArchivedRkyvRequest::Write { inode, .. } => write!(f, "Write: {}", inode),
            ArchivedRkyvRequest::Read { inode, .. } => write!(f, "Read: {}", inode),
            ArchivedRkyvRequest::ReadRaw { inode, .. } => write!(f, "ReadRaw: {}", inode),
            ArchivedRkyvRequest::LatestCommit { raft_group } => {
                write!(f, "LatestCommit: {}", raft_group)
            }
            ArchivedRkyvRequest::RaftGroupLeader { raft_group } => {
                write!(f, "RaftGroupLeader: {}", raft_group)
            }
            ArchivedRkyvRequest::RaftMessage { raft_group, .. } => {
                write!(f, "RaftMessage: {}", raft_group)
            }
            ArchivedRkyvRequest::Lock { inode } => write!(f, "Lock: {}", inode),
            ArchivedRkyvRequest::Unlock { inode, lock_id } => {
                write!(f, "Unlock: {}, {}", inode, lock_id)
            }
            ArchivedRkyvRequest::HardlinkRollback { inode, .. } => {
                write!(f, "HardlinkRollback: {}", inode)
            }
            ArchivedRkyvRequest::HardlinkIncrement { inode, .. } => {
                write!(f, "HardlinkIncrement: {}", inode)
            }
            ArchivedRkyvRequest::DecrementInode { inode, .. } => {
                write!(f, "DecrementInode: {}", inode)
            }
            ArchivedRkyvRequest::UpdateParent { inode, .. } => {
                write!(f, "UpdateParent: {}", inode)
            }
            ArchivedRkyvRequest::UpdateMetadataChangedTime { inode, .. } => {
                write!(f, "UpdateMetadataChangedTime: {}", inode)
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
            ArchivedRkyvRequest::ReadRaw { inode, .. }
            | ArchivedRkyvRequest::Read { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::ReadData,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::SetXattr { inode, .. }
            | ArchivedRkyvRequest::RemoveXattr { inode, .. }
            | ArchivedRkyvRequest::Unlink { parent: inode, .. }
            | ArchivedRkyvRequest::Rmdir { parent: inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::Hardlink { .. } | ArchivedRkyvRequest::Rename { .. } => {
                RequestMetaInfo {
                    raft_group: None,
                    inode: None,
                    lock_id: None,
                    access_type: AccessType::WriteMetadata,
                    distribution_requirement: DistributionRequirement::TransactionCoordinator,
                }
            }
            ArchivedRkyvRequest::Write { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::WriteDataAndMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::ListXattrs { inode }
            | ArchivedRkyvRequest::GetXattr { inode, .. }
            | ArchivedRkyvRequest::Lookup { parent: inode, .. }
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
            ArchivedRkyvRequest::CreateInode { raft_group, .. } => RequestMetaInfo {
                raft_group: Some(raft_group.into()),
                inode: None,
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::Mkdir { parent, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(parent.into()),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::TransactionCoordinator,
            },
            ArchivedRkyvRequest::Create { parent, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(parent.into()),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::TransactionCoordinator,
            },
            ArchivedRkyvRequest::RemoveLink {
                parent: inode,
                lock_id,
                ..
            }
            | ArchivedRkyvRequest::CreateLink {
                parent: inode,
                lock_id,
                ..
            }
            | ArchivedRkyvRequest::DecrementInode { inode, lock_id, .. }
            | ArchivedRkyvRequest::UpdateParent { inode, lock_id, .. }
            | ArchivedRkyvRequest::UpdateMetadataChangedTime { inode, lock_id, .. }
            | ArchivedRkyvRequest::ReplaceLink {
                parent: inode,
                lock_id,
                ..
            } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: lock_id.as_ref().map(|x| x.into()),
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::Truncate { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::WriteDataAndMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            ArchivedRkyvRequest::HardlinkRollback { inode, .. }
            | ArchivedRkyvRequest::Chown { inode, .. }
            | ArchivedRkyvRequest::Chmod { inode, .. }
            | ArchivedRkyvRequest::HardlinkIncrement { inode, .. }
            | ArchivedRkyvRequest::Utimens { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(inode.into()),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
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
                    kind: (&entry.kind).into(),
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
