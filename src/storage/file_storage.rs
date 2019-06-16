use std::ffi::CString;
use std::fs;
use std::fs::File;
use std::os::linux::fs::MetadataExt;
use std::path::{Path, PathBuf};

use filetime::FileTime;
use flatbuffers::{FlatBufferBuilder, Vector, WIPOffset};
use log::info;
use log::warn;

use crate::generated::*;
use crate::storage::data_storage::BLOCK_SIZE;
use crate::storage::metadata_storage::MetadataStorage;
use crate::utils::{empty_response, into_error_code, ResultResponse};

// Handles one request/response
pub struct FileStorage<'a> {
    pub path: String,
    local_data_dir: String,
    response_buffer: FlatBufferBuilder<'a>,
}

impl<'a> FileStorage<'a> {
    pub fn new(
        path: String,
        local_data_dir: String,
        builder: FlatBufferBuilder<'a>,
    ) -> FileStorage<'a> {
        FileStorage {
            // XXX: hack
            path: path.trim_start_matches('/').to_string(),
            local_data_dir,
            response_buffer: builder,
        }
    }

    fn to_local_path(&self, path: &str) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    pub fn truncate(self, new_length: u64) -> ResultResponse<'a> {
        let local_path = self.to_local_path(&self.path);
        let file = File::create(&local_path).expect("Couldn't create file");
        file.set_len(new_length).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }

    pub fn mkdir(self, _mode: u16) -> Result<FlatBufferBuilder<'a>, ErrorCode> {
        assert_ne!(self.path.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.path);
        fs::create_dir(path).map_err(into_error_code)?;

        // TODO set the mode

        return Ok(self.response_buffer);
    }

    pub fn readdir(mut self) -> ResultResponse<'a> {
        let path = Path::new(&self.local_data_dir).join(&self.path);

        let mut entries = vec![];
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let filename = entry.file_name().clone().to_str().unwrap().to_string();
            let file_type = if entry.file_type().unwrap().is_file() {
                FileKind::File
            } else if entry.file_type().unwrap().is_dir() {
                FileKind::Directory
            } else {
                unimplemented!()
            };
            let path = self.response_buffer.create_string(&filename);
            let directory_entry = DirectoryEntry::create(
                &mut self.response_buffer,
                &DirectoryEntryArgs {
                    path: Some(path),
                    kind: file_type,
                },
            );
            entries.push(directory_entry);
        }
        self.response_buffer
            .start_vector::<WIPOffset<DirectoryEntry>>(entries.len());
        for &directory_entry in entries.iter() {
            self.response_buffer.push(directory_entry);
        }
        let entries = self.response_buffer.end_vector(entries.len());
        let mut builder = DirectoryListingResponseBuilder::new(&mut self.response_buffer);
        builder.add_entries(entries);

        let offset = builder.finish().as_union_value();
        return Ok((
            self.response_buffer,
            ResponseType::DirectoryListingResponse,
            offset,
        ));
    }

    pub fn access(
        self,
        uid: u32,
        gids: &Vector<'_, u32>,
        mut mask: u32,
        metadata_storage: &MetadataStorage,
    ) -> ResultResponse<'a> {
        if self.path.is_empty() {
            // TODO: currently let everyone access the root
            return empty_response(self.response_buffer);
        }

        let path = self.to_local_path(&self.path);
        let metadata = fs::metadata(path).map_err(into_error_code)?;

        // F_OK tests for existence of file
        if mask == libc::F_OK as u32 {
            return empty_response(self.response_buffer);
        }

        // TODO: hacky, because metadata storage only receives changes via chown. Not the original owner
        let file_uid = metadata_storage
            .get_uid(&self.path)
            .unwrap_or_else(|| metadata.st_uid());
        let file_gid = metadata_storage
            .get_gid(&self.path)
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

        return empty_response(self.response_buffer);
    }

    pub fn getattr(mut self, metadata_storage: &MetadataStorage) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.path);
        let metadata = fs::metadata(path).map_err(into_error_code)?;

        let length = metadata_storage.get_length(&self.path).unwrap();

        let mut builder = FileMetadataResponseBuilder::new(&mut self.response_buffer);
        builder.add_size_bytes(length);
        builder.add_size_blocks(length / BLOCK_SIZE);
        let atime = Timestamp::new(metadata.st_atime(), metadata.st_atime_nsec() as i32);
        builder.add_last_access_time(&atime);
        let mtime = Timestamp::new(metadata.st_mtime(), metadata.st_mtime_nsec() as i32);
        builder.add_last_modified_time(&mtime);
        let ctime = Timestamp::new(metadata.st_ctime(), metadata.st_ctime_nsec() as i32);
        builder.add_last_metadata_modified_time(&ctime);
        if metadata.is_file() {
            builder.add_kind(FileKind::File);
        } else if metadata.is_dir() {
            builder.add_kind(FileKind::Directory);
        } else {
            unimplemented!();
        }
        builder.add_mode(metadata.st_mode() as u16);
        builder.add_hard_links(metadata.st_nlink() as u32);
        // TODO: hacky, because metadata storage only receives changes via chown. Not the original owner
        if let Some(uid) = metadata_storage.get_uid(&self.path) {
            builder.add_user_id(uid);
        } else {
            builder.add_user_id(metadata.st_uid());
        }
        if let Some(gid) = metadata_storage.get_gid(&self.path) {
            builder.add_group_id(gid);
        } else {
            builder.add_group_id(metadata.st_gid());
        }
        builder.add_device_id(metadata.st_rdev() as u32);

        let offset = builder.finish().as_union_value();
        return Ok((
            self.response_buffer,
            ResponseType::FileMetadataResponse,
            offset,
        ));
    }

    pub fn utimens(
        self,
        uid: u32,
        atime_secs: i64,
        atime_nanos: i32,
        mtime_secs: i64,
        mtime_nanos: i32,
        metadata_storage: &MetadataStorage,
    ) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
        let metadata = fs::metadata(&local_path).map_err(into_error_code)?;

        // TODO: hacky, because metadata storage only receives changes via chown. Not the original owner
        let file_uid = metadata_storage
            .get_uid(&self.path)
            .unwrap_or_else(|| metadata.st_uid());

        // Non-owners are only allowed to change atime & mtime to current:
        // http://man7.org/linux/man-pages/man2/utimensat.2.html
        if file_uid != uid
            && uid != 0
            && (atime_nanos != libc::UTIME_NOW as i32 || mtime_nanos != libc::UTIME_NOW as i32)
        {
            return Err(ErrorCode::OperationNotPermitted);
        }

        filetime::set_file_times(
            local_path,
            FileTime::from_unix_time(atime_secs, atime_nanos as u32),
            FileTime::from_unix_time(mtime_secs, mtime_nanos as u32),
        )
        .map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }

    pub fn chmod(self, mode: u32) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
        let c_path =
            CString::new(local_path.to_str().unwrap().as_bytes()).expect("CString creation failed");
        let exit_code;
        unsafe { exit_code = libc::chmod(c_path.as_ptr(), mode) }
        if exit_code == libc::EXIT_SUCCESS {
            return empty_response(self.response_buffer);
        } else {
            warn!(
                "chmod failed on {:?}, {} with error {:?}",
                local_path, mode, exit_code
            );
            return Err(ErrorCode::Uncategorized);
        }
    }

    pub fn hardlink(mut self, new_path: &str) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!("Hardlinking file: {} to {}", self.path, new_path);
        let local_path = self.to_local_path(&self.path);
        let local_new_path = self.to_local_path(new_path);
        // TODO error handling
        fs::hard_link(&local_path, &local_new_path).expect("hardlink failed");
        let metadata = fs::metadata(local_new_path).map_err(into_error_code)?;

        let mut builder = FileMetadataResponseBuilder::new(&mut self.response_buffer);
        builder.add_size_bytes(metadata.len());
        builder.add_size_blocks(metadata.st_blocks());
        let atime = Timestamp::new(metadata.st_atime(), metadata.st_atime_nsec() as i32);
        builder.add_last_access_time(&atime);
        let mtime = Timestamp::new(metadata.st_mtime(), metadata.st_mtime_nsec() as i32);
        builder.add_last_modified_time(&mtime);
        let ctime = Timestamp::new(metadata.st_ctime(), metadata.st_ctime_nsec() as i32);
        builder.add_last_metadata_modified_time(&ctime);
        if metadata.is_file() {
            builder.add_kind(FileKind::File);
        } else if metadata.is_dir() {
            builder.add_kind(FileKind::Directory);
        } else {
            unimplemented!();
        }
        builder.add_mode(metadata.st_mode() as u16);
        builder.add_hard_links(metadata.st_nlink() as u32);
        builder.add_user_id(metadata.st_uid());
        builder.add_group_id(metadata.st_gid());
        builder.add_device_id(metadata.st_rdev() as u32);

        let offset = builder.finish().as_union_value();
        return Ok((
            self.response_buffer,
            ResponseType::FileMetadataResponse,
            offset,
        ));
    }

    pub fn rename(self, new_path: &str) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!("Renaming file: {} to {}", self.path, new_path);
        let local_path = self.to_local_path(&self.path);
        let local_new_path = self.to_local_path(new_path);
        fs::rename(local_path, local_new_path).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }

    pub fn unlink(self) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!("Deleting file");
        let local_path = self.to_local_path(&self.path);
        fs::remove_file(local_path).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }
}
