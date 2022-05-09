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
}

// Add some helper methods to the generated rkyv type for RkyvGenericResponse
impl ArchivedRkyvGenericResponse {
    pub fn as_inode_response(&self) -> Option<u64> {
        if let ArchivedRkyvGenericResponse::Inode { id } = self {
            Some(id.into())
        } else {
            None
        }
    }
}
