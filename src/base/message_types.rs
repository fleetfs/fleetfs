use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

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
    Flatbuffer(Vec<u8>),
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
    Empty,
    ErrorOccurred(ErrorCode),
    // Mapping from raft group ids to their checksum
    Checksums(HashMap<u16, Vec<u8>>),
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
