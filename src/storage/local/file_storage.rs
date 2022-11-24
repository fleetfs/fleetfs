use log::info;
use std::fs;

use crate::base::node_id_from_address;
use crate::base::{
    CommitId, DirectoryEntry, EntryMetadata, ErrorCode, FileKind, RkyvGenericResponse, Timestamp,
    UserContext,
};
use crate::client::TcpPeerClient;
use crate::storage::local::data_storage::{DataStorage, BLOCK_SIZE};
use crate::storage::local::error_helper::into_error_code;
use crate::storage::local::metadata_storage::{InodeAttributes, MetadataStorage, MAX_NAME_LENGTH};
use crate::storage::ROOT_INODE;
use futures::Future;
use futures::FutureExt;
use std::net::SocketAddr;
use std::path::Path;

fn build_fileattr_response(
    attributes: InodeAttributes,
    directory_entries: Option<u32>,
) -> EntryMetadata {
    EntryMetadata {
        inode: attributes.inode,
        size_bytes: attributes.size,
        size_blocks: attributes.blocks(),
        last_access_time: attributes.last_accessed,
        last_modified_time: attributes.last_modified,
        last_metadata_modified_time: attributes.last_metadata_changed,
        kind: attributes.kind,
        mode: attributes.mode,
        hard_links: attributes.hardlinks,
        user_id: attributes.uid,
        group_id: attributes.gid,
        device_id: 0,
        block_size: BLOCK_SIZE as u32,
        directory_entries,
    }
}

fn to_fileattr_response(
    attributes: InodeAttributes,
    directory_entries: Option<u32>,
) -> RkyvGenericResponse {
    RkyvGenericResponse::EntryMetadata(build_fileattr_response(attributes, directory_entries))
}

pub struct FileStorage {
    data_storage: DataStorage<TcpPeerClient>,
    metadata_storage: MetadataStorage,
}

impl FileStorage {
    pub fn new(
        node_id: u64,
        raft_group: u16,
        num_raft_groups: u16,
        storage_dir: &Path,
        peers: &[SocketAddr],
    ) -> FileStorage {
        let peer_clients = peers
            .iter()
            .map(|peer| (node_id_from_address(peer), TcpPeerClient::new(*peer)))
            .collect();
        let data_dir = storage_dir.join("data");
        let metadata_dir = storage_dir.join("metadata");
        fs::create_dir_all(&data_dir)
            .unwrap_or_else(|_| panic!("Failed to create data dir: {:?}", &data_dir));
        fs::create_dir_all(&metadata_dir)
            .unwrap_or_else(|_| panic!("Failed to create metadata dir: {:?}", &metadata_dir));
        FileStorage {
            data_storage: DataStorage::new(node_id, data_dir.to_str().unwrap(), peer_clients),
            metadata_storage: MetadataStorage::new(raft_group, num_raft_groups, &metadata_dir),
        }
    }

    pub fn local_data_checksum(&self) -> Result<Vec<u8>, ErrorCode> {
        // TODO: this only checks the integrity of metadata & plain files. Directories are purely
        // stored in the metadata_storage
        for inode in self.metadata_storage.non_directory_inodes()? {
            if !self.data_storage.file_inode_exists(inode) {
                return Err(ErrorCode::Corrupted);
            }
        }

        self.data_storage
            .local_data_checksum()
            .map_err(|_| ErrorCode::Uncategorized)
    }

    pub fn statfs(&self) -> RkyvGenericResponse {
        RkyvGenericResponse::FilesystemInformation {
            block_size: BLOCK_SIZE as u32,
            max_name_length: MAX_NAME_LENGTH,
        }
    }

    pub fn lookup(
        &self,
        parent: u64,
        name: &str,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        let maybe_inode = self.metadata_storage.lookup(parent, name, context)?;

        if let Some(inode) = maybe_inode {
            Ok(RkyvGenericResponse::Inode { id: inode })
        } else {
            Err(ErrorCode::DoesNotExist)
        }
    }

    pub fn truncate(
        &self,
        inode: u64,
        new_length: u64,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage.truncate(inode, new_length, context)?;
        self.data_storage.truncate(inode, new_length).unwrap();

        Ok(RkyvGenericResponse::Empty)
    }

    pub fn readdir(&self, inode: u64) -> Result<RkyvGenericResponse, ErrorCode> {
        let mut entries = vec![];
        for (inode, filename, file_type) in self.metadata_storage.readdir(inode)? {
            entries.push(DirectoryEntry {
                inode,
                name: filename,
                kind: file_type,
            });
        }

        Ok(RkyvGenericResponse::DirectoryListing(entries))
    }

    pub fn getattr(&self, inode: u64) -> Result<RkyvGenericResponse, ErrorCode> {
        let (attributes, directory_entries) = self.metadata_storage.get_attributes(inode)?;
        Ok(to_fileattr_response(attributes, directory_entries))
    }

    pub fn utimens(
        &self,
        inode: u64,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        self.metadata_storage
            .utimens(inode, atime, mtime, context)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn chmod(
        &self,
        inode: u64,
        mode: u32,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        self.metadata_storage.chmod(inode, mode, context)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn chown(
        &self,
        inode: u64,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        self.metadata_storage.chown(inode, uid, gid, context)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn fsync(&self, inode: u64) -> Result<RkyvGenericResponse, ErrorCode> {
        self.data_storage.fsync(inode)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn read(
        &self,
        inode: u64,
        offset: u64,
        read_size: u32,
        required_commit: CommitId,
    ) -> impl Future<Output = Result<RkyvGenericResponse, ErrorCode>> + '_ {
        // No access check is needed, since we rely on the client to do it
        let read_result = self
            .data_storage
            .read(inode, offset, read_size, required_commit);
        read_result.map(move |response| response.map(|data| RkyvGenericResponse::Read { data }))
    }

    pub fn read_raw(
        &self,
        inode: u64,
        offset: u64,
        read_size: u32,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        let data = self
            .data_storage
            .read_raw(inode, offset, read_size)
            .map_err(into_error_code)?;
        Ok(RkyvGenericResponse::Read { data })
    }

    pub fn write(
        &self,
        inode: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage
            .write(inode, offset, data.len() as u32)?;
        let write_result = self.data_storage.write_local_blocks(inode, offset, data);
        // Reply with the total requested write size, since that's what the FUSE client is expecting, even though this node only wrote some of the bytes
        let total_bytes = data.len() as u32;
        write_result
            .map(move |_| RkyvGenericResponse::Written {
                bytes_written: total_bytes,
            })
            .map_err(into_error_code)
    }

    pub fn hardlink_stage0_link_increment(
        &self,
        inode: u64,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        let rollback = self
            .metadata_storage
            .hardlink_stage0_link_increment(inode)?;
        let (attributes, directory_size) = self.metadata_storage.get_attributes(inode)?;
        let attrs = build_fileattr_response(attributes, directory_size);
        Ok(RkyvGenericResponse::HardlinkTransaction {
            rollback_last_modified: rollback,
            attrs,
        })
    }

    pub fn create_link(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
        inode_kind: FileKind,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage
            .create_link(inode, new_parent, new_name, context, inode_kind)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn replace_link(
        &self,
        parent: u64,
        name: &str,
        new_inode: u64,
        kind: FileKind,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        let old_inode = self
            .metadata_storage
            .replace_link(parent, name, new_inode, kind, context)?;
        Ok(RkyvGenericResponse::Inode { id: old_inode })
    }

    pub fn update_parent(
        &self,
        inode: u64,
        new_parent: u64,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage.update_parent(inode, new_parent)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn update_metadata_changed_time(
        &self,
        inode: u64,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage.update_metadata_changed_time(inode)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn hardlink_rollback(
        &self,
        inode: u64,
        last_metadata_changed: Timestamp,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage
            .hardlink_rollback(inode, last_metadata_changed)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn get_xattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        let attr = self.metadata_storage.get_xattr(inode, key, context)?;
        Ok(RkyvGenericResponse::Read { data: attr })
    }

    pub fn list_xattrs(&self, inode: u64) -> Result<RkyvGenericResponse, ErrorCode> {
        Ok(RkyvGenericResponse::Xattrs {
            attrs: self.metadata_storage.list_xattrs(inode)?,
        })
    }

    pub fn set_xattr(
        &self,
        inode: u64,
        key: &str,
        value: &[u8],
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage
            .set_xattr(inode, key, value, context)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn remove_xattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        self.metadata_storage.remove_xattr(inode, key, context)?;
        Ok(RkyvGenericResponse::Empty)
    }

    pub fn remove_link(
        &self,
        parent: u64,
        name: &str,
        link_inode_and_uid: Option<(u64, u32)>,
        context: UserContext,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        info!("Deleting file");
        let (deleted_inode, processed) =
            self.metadata_storage
                .remove_link(parent, name, link_inode_and_uid, context)?;
        Ok(RkyvGenericResponse::RemovedInode {
            id: deleted_inode,
            complete: processed,
        })
    }

    pub fn decrement_inode_link_count(
        &self,
        inode: u64,
        count: u32,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        if let Some(deleted_inode) = self
            .metadata_storage
            .decrement_inode_link_count(inode, count)?
        {
            self.data_storage.delete(deleted_inode).unwrap();
        }

        Ok(RkyvGenericResponse::Empty)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_inode(
        &self,
        parent: u64,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    ) -> Result<RkyvGenericResponse, ErrorCode> {
        let (_, attributes) = self
            .metadata_storage
            .create_inode(parent, uid, gid, mode, kind)?;

        if kind != FileKind::Directory {
            self.data_storage.truncate(attributes.inode, 0).unwrap();
        }

        let directory_entries = if kind == FileKind::Directory {
            Some(0)
        } else {
            None
        };

        Ok(to_fileattr_response(attributes, directory_entries))
    }
}
