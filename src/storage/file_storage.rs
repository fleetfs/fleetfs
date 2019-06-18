use std::fs;
use std::fs::File;
use std::os::linux::fs::MetadataExt;
use std::path::{Path, PathBuf};

use flatbuffers::{FlatBufferBuilder, Vector, WIPOffset};
use log::info;

use crate::generated::*;
use crate::storage::data_storage::{DataStorage, BLOCK_SIZE};
use crate::storage::metadata_storage::MetadataStorage;
use crate::storage::ROOT_INODE;
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, into_error_code, ResultResponse};

pub struct FileStorage {
    local_data_dir: String,
    data_storage: DataStorage,
    metadata_storage: MetadataStorage,
}

impl FileStorage {
    pub fn new(node_id: u64, all_node_ids: &[u64], context: &LocalContext) -> FileStorage {
        FileStorage {
            // XXX: hack
            local_data_dir: context.data_dir.clone(),
            data_storage: DataStorage::new(node_id, all_node_ids, context),
            metadata_storage: MetadataStorage::new(),
        }
    }

    fn to_local_path(&self, path: &str) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
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
        path: &str,
        new_length: u64,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        self.metadata_storage.truncate(&path, new_length);

        let local_path = self.to_local_path(path);
        let file = File::create(&local_path).expect("Couldn't create file");
        file.set_len(new_length).map_err(into_error_code)?;

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

        let path = Path::new(&self.local_data_dir).join(path);
        fs::create_dir(path).map_err(into_error_code)?;

        // TODO set the mode

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
                    path: Some(name),
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

    pub fn access<'a>(
        &self,
        path: &str,
        uid: u32,
        gids: &Vector<'_, u32>,
        mut mask: u32,
        metadata_storage: &MetadataStorage,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        if path.is_empty() {
            // TODO: currently let everyone access the root
            return empty_response(builder);
        }

        let local_path = self.to_local_path(path);
        let metadata = fs::metadata(local_path).map_err(into_error_code)?;

        // F_OK tests for existence of file
        if mask == libc::F_OK as u32 {
            return empty_response(builder);
        }

        // TODO: hacky, because metadata storage only receives changes via chown. Not the original owner
        let file_uid = metadata_storage
            .get_uid(path)
            .unwrap_or_else(|| metadata.st_uid());
        let file_gid = metadata_storage
            .get_gid(path)
            .unwrap_or_else(|| metadata.st_gid());

        let mode = metadata.st_mode();
        // Process "other" permissions
        mask -= mask & mode;
        for i in 0..gids.len() {
            if gids.get(i) == file_gid {
                mask -= mask & (mode >> 3);
                break;
            }
        }
        if file_uid == uid {
            mask -= mask & (mode >> 6);
        }

        if mask != 0 {
            return Err(ErrorCode::AccessDenied);
        }

        return empty_response(builder);
    }

    pub fn getattr<'a>(
        &self,
        inode: u64,
        mut builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        if let Some(attributes) = self.metadata_storage.get_attributes(inode) {
            let mut response_builder = FileMetadataResponseBuilder::new(&mut builder);
            response_builder.add_size_bytes(attributes.size);
            response_builder.add_size_blocks(attributes.size / BLOCK_SIZE);
            response_builder.add_last_access_time(&attributes.last_accessed);
            response_builder.add_last_modified_time(&attributes.last_modified);
            response_builder.add_last_metadata_modified_time(&attributes.last_metadata_changed);
            response_builder.add_kind(attributes.kind);
            response_builder.add_mode(attributes.mode);
            response_builder.add_hard_links(attributes.hardlinks);
            response_builder.add_user_id(attributes.uid);
            response_builder.add_group_id(attributes.gid);
            response_builder.add_device_id(0); // TODO

            let offset = response_builder.finish().as_union_value();
            return Ok((builder, ResponseType::FileMetadataResponse, offset));
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
        path: &str,
        mode: u32,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);
        self.metadata_storage.chmod(path, mode);
        return empty_response(builder);
    }

    pub fn hardlink<'a>(
        &self,
        path: &str,
        new_path: &str,
        mut builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);

        self.metadata_storage.hardlink(&path, &new_path);

        info!("Hardlinking file: {} to {}", path, new_path);
        let local_path = self.to_local_path(path);
        let local_new_path = self.to_local_path(new_path);
        // TODO error handling
        fs::hard_link(&local_path, &local_new_path).expect("hardlink failed");
        let metadata = fs::metadata(local_new_path).map_err(into_error_code)?;

        let mut response_builder = FileMetadataResponseBuilder::new(&mut builder);
        response_builder.add_size_bytes(metadata.len());
        response_builder.add_size_blocks(metadata.st_blocks());
        let atime = Timestamp::new(metadata.st_atime(), metadata.st_atime_nsec() as i32);
        response_builder.add_last_access_time(&atime);
        let mtime = Timestamp::new(metadata.st_mtime(), metadata.st_mtime_nsec() as i32);
        response_builder.add_last_modified_time(&mtime);
        let ctime = Timestamp::new(metadata.st_ctime(), metadata.st_ctime_nsec() as i32);
        response_builder.add_last_metadata_modified_time(&ctime);
        if metadata.is_file() {
            response_builder.add_kind(FileKind::File);
        } else if metadata.is_dir() {
            response_builder.add_kind(FileKind::Directory);
        } else {
            unimplemented!();
        }
        response_builder.add_mode(metadata.st_mode() as u16);
        response_builder.add_hard_links(metadata.st_nlink() as u32);
        response_builder.add_user_id(metadata.st_uid());
        response_builder.add_group_id(metadata.st_gid());
        response_builder.add_device_id(metadata.st_rdev() as u32);

        let offset = response_builder.finish().as_union_value();
        return Ok((builder, ResponseType::FileMetadataResponse, offset));
    }

    pub fn rename<'a>(
        &self,
        path: &str,
        new_path: &str,
        builder: FlatBufferBuilder<'a>,
    ) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);

        self.metadata_storage.rename(&path, &new_path);

        info!("Renaming file: {} to {}", path, new_path);
        let local_path = self.to_local_path(path);
        let local_new_path = self.to_local_path(new_path);
        fs::rename(local_path, local_new_path).map_err(into_error_code)?;

        return empty_response(builder);
    }

    pub fn unlink<'a>(&self, path: &str, builder: FlatBufferBuilder<'a>) -> ResultResponse<'a> {
        assert_ne!(path.len(), 0);

        self.metadata_storage.unlink(&path);

        info!("Deleting file");
        let local_path = self.to_local_path(path);
        fs::remove_file(local_path).map_err(into_error_code)?;

        return empty_response(builder);
    }
}
