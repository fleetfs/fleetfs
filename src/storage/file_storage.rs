use flatbuffers::{FlatBufferBuilder, WIPOffset};
use log::info;

use crate::generated::*;
use crate::storage::data_storage::DataStorage;
use crate::storage::metadata_storage::MetadataStorage;
use crate::storage::ROOT_INODE;
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, into_error_code, to_fileattr_response, ResultResponse};

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

    // TODO: this should return an inode
    pub fn lookup(&self, path: &str) -> String {
        path.trim_start_matches('/').to_string()
    }

    pub fn truncate<'a>(
        &self,
        inode: u64,
        new_length: u64,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.truncate(inode, new_length);
        self.data_storage
            .truncate(inode, new_length)
            .map_err(into_error_code)?;

        return empty_response(builder);
    }

    pub fn mkdir<'a>(
        &self,
        path: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        builder: FlatBufferBuilder<'a>,
    ) -> Result<FlatBufferBuilder<'a>, ErrorCode> {
        assert_ne!(path.len(), 0);

        self.metadata_storage.mkdir(&path, uid, gid, mode);

        return Ok(builder);
    }

    pub fn readdir<'a>(
        &self,
        inode: u64,
        mut builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let mut entries = vec![];
        for (filename, file_type) in self.metadata_storage.readdir(inode)? {
            let name = builder.create_string(&filename);
            let directory_entry = DirectoryEntry::create(
                &mut builder,
                &DirectoryEntryArgs {
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
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(inode, ROOT_INODE);
        self.metadata_storage.chmod(inode, mode);
        return empty_response(builder);
    }

    pub fn hardlink<'a>(
        &self,
        path: &str,
        new_path: &str,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);
        info!("Hardlinking file: {} to {}", path, new_path);

        self.metadata_storage.hardlink(&path, &new_path);
        let inode = self.metadata_storage.lookup_path(path).unwrap();
        return self.getattr(inode, builder);
    }

    pub fn rename<'a>(
        &self,
        path: &str,
        new_path: &str,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);
        self.metadata_storage.rename(&path, &new_path);
        return empty_response(builder);
    }

    pub fn unlink<'a>(&self, path: &str, builder: FlatBufferBuilder<'a>) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);

        info!("Deleting file");
        if let Some(deleted_inode) = self.metadata_storage.unlink(&path) {
            self.data_storage.delete(deleted_inode)?;
        }

        return empty_response(builder);
    }

    pub fn create<'a>(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        let (_, attributes) = self
            .metadata_storage
            .create(parent, name, uid, gid, mode)
            .unwrap();

        return to_fileattr_response(builder, attributes);
    }
}
