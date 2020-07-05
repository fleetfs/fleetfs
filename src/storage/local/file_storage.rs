use flatbuffers::{FlatBufferBuilder, WIPOffset};
use log::info;

use crate::base::{empty_response, node_id_from_address, FlatBufferWithResponse, ResultResponse};
use crate::client::TcpPeerClient;
use crate::generated::*;
use crate::storage::local::data_storage::{DataStorage, BLOCK_SIZE};
use crate::storage::local::metadata_storage::{InodeAttributes, MetadataStorage, MAX_NAME_LENGTH};
use crate::storage::local::response_helpers::{
    into_error_code, to_fast_read_response, to_inode_response, to_read_response, to_write_response,
    to_xattrs_response,
};
use crate::storage::ROOT_INODE;
use futures::Future;
use futures::FutureExt;
use std::net::SocketAddr;

pub fn remove_link_response(
    mut buffer: FlatBufferBuilder,
    inode: u64,
    processed: bool,
) -> ResultResponse {
    let mut response_builder = RemoveLinkResponseBuilder::new(&mut buffer);
    response_builder.add_inode(inode);
    response_builder.add_processing_complete(processed);
    let offset = response_builder.finish().as_union_value();
    return Ok((buffer, ResponseType::RemoveLinkResponse, offset));
}

fn build_fileattr_response<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    attributes: InodeAttributes,
    directory_entries: u32,
) -> WIPOffset<FileMetadataResponse<'a>> {
    let mut response_builder = FileMetadataResponseBuilder::new(builder);
    response_builder.add_inode(attributes.inode);
    response_builder.add_size_bytes(attributes.size);
    response_builder.add_size_blocks(attributes.blocks());
    response_builder.add_last_access_time(&attributes.last_accessed);
    response_builder.add_last_modified_time(&attributes.last_modified);
    response_builder.add_last_metadata_modified_time(&attributes.last_metadata_changed);
    response_builder.add_kind(attributes.kind);
    response_builder.add_mode(attributes.mode);
    response_builder.add_hard_links(attributes.hardlinks);
    response_builder.add_user_id(attributes.uid);
    response_builder.add_group_id(attributes.gid);
    response_builder.add_device_id(0); // TODO
    response_builder.add_block_size(BLOCK_SIZE as u32);
    response_builder.add_directory_entries(directory_entries);

    return response_builder.finish();
}

fn to_fileattr_response(
    mut builder: FlatBufferBuilder,
    attributes: InodeAttributes,
    directory_entries: u32,
) -> ResultResponse {
    let offset =
        build_fileattr_response(&mut builder, attributes, directory_entries).as_union_value();
    return Ok((builder, ResponseType::FileMetadataResponse, offset));
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
        data_dir: &str,
        peers: &[SocketAddr],
    ) -> FileStorage {
        let peer_clients = peers
            .iter()
            .map(|peer| (node_id_from_address(peer), TcpPeerClient::new(*peer)))
            .collect();
        FileStorage {
            data_storage: DataStorage::new(node_id, data_dir, peer_clients),
            metadata_storage: MetadataStorage::new(raft_group, num_raft_groups),
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

    pub fn statfs<'a>(&self, mut builder: FlatBufferBuilder<'a>) -> ResultResponse<'a> {
        let mut response_builder = FilesystemInformationResponseBuilder::new(&mut builder);
        response_builder.add_block_size(BLOCK_SIZE as u32);
        response_builder.add_max_name_length(MAX_NAME_LENGTH);

        let offset = response_builder.finish().as_union_value();
        return Ok((builder, ResponseType::FilesystemInformationResponse, offset));
    }

    pub fn lookup<'a>(
        &self,
        parent: u64,
        name: &str,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let maybe_inode = self.metadata_storage.lookup(parent, name, context)?;

        if let Some(inode) = maybe_inode {
            return to_inode_response(builder, inode);
        } else {
            return Err(ErrorCode::DoesNotExist);
        }
    }

    pub fn truncate<'a>(
        &self,
        inode: u64,
        new_length: u64,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.truncate(inode, new_length, context)?;
        self.data_storage.truncate(inode, new_length).unwrap();

        return empty_response(builder);
    }

    pub fn readdir<'a>(
        &self,
        inode: u64,
        mut builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let mut entries = vec![];
        for (inode, filename, file_type) in self.metadata_storage.readdir(inode)? {
            let name = builder.create_string(&filename);
            let directory_entry = DirectoryEntry::create(
                &mut builder,
                &DirectoryEntryArgs {
                    inode,
                    name: Some(name),
                    kind: file_type,
                },
            );
            entries.push(directory_entry);
        }
        builder.start_vector::<WIPOffset<DirectoryEntry>>(entries.len());
        for &directory_entry in entries.iter() {
            builder.push(directory_entry);
        }
        let entries = builder.end_vector(entries.len());
        let mut response_builder = DirectoryListingResponseBuilder::new(&mut builder);
        response_builder.add_entries(entries);

        let offset = response_builder.finish().as_union_value();
        return Ok((builder, ResponseType::DirectoryListingResponse, offset));
    }

    pub fn getattr<'a>(&self, inode: u64, builder: FlatBufferBuilder<'a>) -> ResultResponse<'a> {
        let (attributes, directory_entries) = self.metadata_storage.get_attributes(inode)?;
        return to_fileattr_response(builder, attributes, directory_entries);
    }

    pub fn utimens<'a>(
        &self,
        inode: u64,
        atime: Option<&Timestamp>,
        mtime: Option<&Timestamp>,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        self.metadata_storage
            .utimens(inode, atime, mtime, context)?;
        return empty_response(builder);
    }

    pub fn chmod<'a>(
        &self,
        inode: u64,
        mode: u32,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        if let Err(error_code) = self.metadata_storage.chmod(inode, mode, context) {
            return Err(error_code);
        } else {
            return empty_response(builder);
        }
    }

    pub fn chown<'a>(
        &self,
        inode: u64,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        if let Err(error_code) = self.metadata_storage.chown(inode, uid, gid, context) {
            return Err(error_code);
        } else {
            return empty_response(builder);
        }
    }

    pub fn fsync<'a>(&self, inode: u64, builder: FlatBufferBuilder<'a>) -> ResultResponse<'a> {
        if let Err(error_code) = self.data_storage.fsync(inode) {
            return Err(error_code);
        } else {
            return empty_response(builder);
        }
    }

    pub fn read(
        &self,
        inode: u64,
        offset: u64,
        read_size: u32,
        required_commit: CommitId,
        builder: FlatBufferBuilder<'static>,
    ) -> impl Future<Output = Result<FlatBufferWithResponse<'static>, ErrorCode>> + '_ {
        // No access check is needed, since we rely on the client to do it
        let read_result = self
            .data_storage
            .read(inode, offset, read_size, required_commit);
        read_result.map(move |response| Ok(to_fast_read_response(builder, response)))
    }

    pub fn read_raw<'a>(
        &self,
        inode: u64,
        offset: u64,
        read_size: u32,
        builder: FlatBufferBuilder<'a>,
    ) -> FlatBufferWithResponse<'a> {
        let data = self
            .data_storage
            .read_raw(inode, offset, read_size)
            .map_err(into_error_code);
        return to_fast_read_response(builder, data);
    }

    pub fn write<'a>(
        &self,
        inode: u64,
        offset: u64,
        data: &[u8],
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage
            .write(inode, offset, data.len() as u32)?;
        let write_result = self.data_storage.write_local_blocks(inode, offset, data);
        // Reply with the total requested write size, since that's what the FUSE client is expecting, even though this node only wrote some of the bytes
        let total_bytes = data.len() as u32;
        return write_result
            .map(move |_| to_write_response(builder, total_bytes).unwrap())
            .map_err(into_error_code);
    }

    pub fn hardlink_stage0_link_increment<'a>(
        &self,
        inode: u64,
        mut builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let rollback = self
            .metadata_storage
            .hardlink_stage0_link_increment(inode)?;
        let (attributes, directory_size) = self.metadata_storage.get_attributes(inode)?;
        let attrs_offset = build_fileattr_response(&mut builder, attributes, directory_size);

        let mut response_builder = HardlinkTransactionResponseBuilder::new(&mut builder);
        response_builder.add_last_modified_time(&rollback.0);
        response_builder.add_kind(rollback.1);
        response_builder.add_attr_response(attrs_offset);

        let offset = response_builder.finish().as_union_value();
        return Ok((builder, ResponseType::HardlinkTransactionResponse, offset));
    }

    pub fn create_link<'a>(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
        inode_kind: FileKind,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage
            .create_link(inode, new_parent, new_name, context, inode_kind)?;
        return empty_response(builder);
    }

    pub fn replace_link<'a>(
        &self,
        parent: u64,
        name: &str,
        new_inode: u64,
        kind: FileKind,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let old_inode = self
            .metadata_storage
            .replace_link(parent, name, new_inode, kind, context)?;
        return to_inode_response(builder, old_inode);
    }

    pub fn update_parent<'a>(
        &self,
        inode: u64,
        new_parent: u64,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.update_parent(inode, new_parent)?;
        return empty_response(builder);
    }

    pub fn update_metadata_changed_time<'a>(
        &self,
        inode: u64,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.update_metadata_changed_time(inode)?;
        return empty_response(builder);
    }

    pub fn hardlink_rollback<'a>(
        &self,
        inode: u64,
        last_metadata_changed: Timestamp,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage
            .hardlink_rollback(inode, last_metadata_changed)?;
        return empty_response(builder);
    }

    pub fn get_xattr<'a>(
        &self,
        inode: u64,
        key: &str,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let attr = self.metadata_storage.get_xattr(inode, key)?;
        return to_read_response(builder, &attr);
    }

    pub fn list_xattrs<'a>(
        &self,
        inode: u64,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let attrs = self.metadata_storage.list_xattrs(inode)?;
        return to_xattrs_response(builder, &attrs);
    }

    pub fn set_xattr<'a>(
        &self,
        inode: u64,
        key: &str,
        value: &[u8],
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage
            .set_xattr(inode, key, value, context)?;
        return empty_response(builder);
    }

    pub fn remove_xattr<'a>(
        &self,
        inode: u64,
        key: &str,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.remove_xattr(inode, key)?;
        return empty_response(builder);
    }

    pub fn remove_link<'a>(
        &self,
        parent: u64,
        name: &str,
        link_inode_and_uid: Option<(u64, u32)>,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        info!("Deleting file");
        let (deleted_inode, processed) =
            self.metadata_storage
                .remove_link(parent, name, link_inode_and_uid, context)?;

        return remove_link_response(builder, deleted_inode, processed);
    }

    pub fn decrement_inode_link_count<'a>(
        &self,
        inode: u64,
        count: u32,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        if let Some(deleted_inode) = self
            .metadata_storage
            .decrement_inode_link_count(inode, count)?
        {
            self.data_storage.delete(deleted_inode).unwrap();
        }

        return empty_response(builder);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_inode<'a>(
        &self,
        parent: u64,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let (_, attributes) = self
            .metadata_storage
            .create_inode(parent, uid, gid, mode, kind)?;

        if kind != FileKind::Directory {
            self.data_storage.truncate(attributes.inode, 0).unwrap();
        }

        return to_fileattr_response(builder, attributes, 0);
    }
}
