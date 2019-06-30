use flatbuffers::{FlatBufferBuilder, WIPOffset};
use log::info;

use crate::generated::*;
use crate::storage::data_storage::DataStorage;
use crate::storage::metadata_storage::MetadataStorage;
use crate::storage::ROOT_INODE;
use crate::storage_node::LocalContext;
use crate::utils::{
    empty_response, into_error_code, to_fast_read_response, to_fileattr_response,
    to_inode_response, to_read_response, to_write_response, to_xattrs_response, FlatBufferResponse,
    FlatBufferWithResponse, FutureResultResponse, ResultResponse,
};
use futures::future::err;
use futures::Future;

pub struct FileStorage {
    data_storage: DataStorage,
    metadata_storage: MetadataStorage,
}

impl FileStorage {
    pub fn new(node_id: u64, all_node_ids: &[u64], context: &LocalContext) -> FileStorage {
        FileStorage {
            data_storage: DataStorage::new(node_id, all_node_ids, context),
            metadata_storage: MetadataStorage::new(),
        }
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

    pub fn mkdir<'a>(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.mkdir(parent, name, uid, gid, mode)?;
        let inode = self
            .metadata_storage
            .lookup(parent, name, UserContext::new(uid, gid))?
            .unwrap();

        return self.getattr(inode, builder);
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
        let attributes = self.metadata_storage.get_attributes(inode)?;
        return to_fileattr_response(builder, attributes);
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
        context: UserContext,
        builder: FlatBufferBuilder<'static>,
    ) -> impl Future<Item = FlatBufferResponse<'static>, Error = ErrorCode> {
        let result: Box<FutureResultResponse>;
        if let Err(error_code) = self.metadata_storage.read(inode, context) {
            result = Box::new(err(error_code));
        } else {
            let read_result = self.data_storage.read(inode, offset, read_size);
            result =
                Box::new(read_result.map(move |data| to_read_response(builder, &data).unwrap()));
        }

        return result;
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
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        if let Err(error_code) =
            self.metadata_storage
                .write(inode, offset, data.len() as u32, context)
        {
            return Err(error_code);
        } else {
            let write_result = self.data_storage.write_local_blocks(inode, offset, data);
            // Reply with the total requested write size, since that's what the FUSE client is expecting, even though this node only wrote some of the bytes
            let total_bytes = data.len() as u32;
            return write_result
                .map(move |_| to_write_response(builder, total_bytes).unwrap())
                .map_err(into_error_code);
        }
    }

    pub fn hardlink<'a>(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        info!("Hardlinking file: {} to {} {}", inode, new_parent, new_name);

        self.metadata_storage
            .hardlink(inode, new_parent, new_name, context)?;
        return self.getattr(inode, builder);
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
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.set_xattr(inode, key, value)?;
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

    pub fn rename<'a>(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage
            .rename(parent, name, new_parent, new_name, context)?;
        return empty_response(builder);
    }

    pub fn rmdir<'a>(
        &self,
        parent: u64,
        name: &str,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        info!("Deleting file");
        self.metadata_storage.rmdir(parent, name, context)?;

        return empty_response(builder);
    }

    pub fn unlink<'a>(
        &self,
        parent: u64,
        name: &str,
        context: UserContext,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        info!("Deleting file");
        if let Some(deleted_inode) = self.metadata_storage.unlink(parent, name, context)? {
            self.data_storage.delete(deleted_inode).unwrap();
        }

        return empty_response(builder);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create<'a>(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let (_, attributes) = self
            .metadata_storage
            .create(parent, name, uid, gid, mode, kind)?;

        self.data_storage.truncate(attributes.inode, 0).unwrap();

        return to_fileattr_response(builder, attributes);
    }
}
