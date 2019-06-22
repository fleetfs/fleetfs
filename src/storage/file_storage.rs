use flatbuffers::{FlatBufferBuilder, WIPOffset};
use log::info;

use crate::generated::*;
use crate::storage::data_storage::DataStorage;
use crate::storage::metadata_storage::MetadataStorage;
use crate::storage::ROOT_INODE;
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, to_fileattr_response, ResultResponse};

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

    // TODO: remove this
    pub fn get_data_storage(&self) -> &DataStorage {
        &self.data_storage
    }

    // TODO: remove this
    pub fn get_metadata_storage(&self) -> &MetadataStorage {
        &self.metadata_storage
    }

    pub fn truncate<'a>(
        &self,
        inode: u64,
        new_length: u64,
        uid: u32,
        gid: u32,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage
            .truncate(inode, new_length, uid, gid)?;
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
        self.metadata_storage.mkdir(parent, name, uid, gid, mode);
        let inode = self
            .metadata_storage
            .lookup(parent, name, uid, gid)?
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
        if let Some(attributes) = self.metadata_storage.get_attributes(inode) {
            return to_fileattr_response(builder, attributes);
        } else {
            return Err(ErrorCode::DoesNotExist);
        }
    }

    pub fn utimens<'a>(
        &self,
        inode: u64,
        uid: u32,
        atime: Option<&Timestamp>,
        mtime: Option<&Timestamp>,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        self.metadata_storage.utimens(inode, uid, atime, mtime)?;
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

    pub fn hardlink<'a>(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        info!("Hardlinking file: {} to {} {}", inode, new_parent, new_name);

        self.metadata_storage.hardlink(inode, new_parent, new_name);
        return self.getattr(inode, builder);
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
        if let Err(error_code) = self
            .metadata_storage
            .rename(parent, name, new_parent, new_name, context)
        {
            return Err(error_code);
        } else {
            return empty_response(builder);
        }
    }

    pub fn unlink<'a>(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        info!("Deleting file");
        if let Some(deleted_inode) = self.metadata_storage.unlink(parent, name, uid, gid)? {
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
