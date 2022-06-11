use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

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
    Written {
        bytes_written: u32,
    },
    LatestCommit {
        term: u64,
        index: u64,
    },
}

// Add some helper methods to the generated rkyv type for RkyvGenericResponse
impl ArchivedRkyvGenericResponse {
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
