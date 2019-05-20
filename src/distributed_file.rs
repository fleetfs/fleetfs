use std::ffi::CString;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::linux::fs::MetadataExt;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

use filetime::FileTime;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use log::info;
use log::warn;

use crate::generated::*;
use crate::utils::{empty_response, into_error_code, ResultResponse};

// Handles one request/response
// TODO: rename this, since it is no longer distributed
pub struct DistributedFileResponder<'a> {
    path: String,
    local_data_dir: String,
    response_buffer: FlatBufferBuilder<'a>,
}

impl<'a> DistributedFileResponder<'a> {
    pub fn new(
        path: String,
        local_data_dir: String,
        builder: FlatBufferBuilder<'a>,
    ) -> DistributedFileResponder<'a> {
        DistributedFileResponder {
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

    pub fn write(mut self, offset: u64, data: &[u8]) -> ResultResponse<'a> {
        let local_path = self.to_local_path(&self.path);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&local_path)
            .map_err(into_error_code)?;

        file.seek(SeekFrom::Start(offset))
            .map_err(into_error_code)?;

        file.write_all(data).map_err(into_error_code)?;
        let mut response_builder = WrittenResponseBuilder::new(&mut self.response_buffer);
        response_builder.add_bytes_written(data.len() as u32);
        let offset = response_builder.finish().as_union_value();
        return Ok((self.response_buffer, ResponseType::WrittenResponse, offset));
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

    pub fn getattr(mut self) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.path);
        let metadata = fs::metadata(path).map_err(into_error_code)?;

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

    pub fn utimens(
        self,
        atime_secs: i64,
        atime_nanos: i32,
        mtime_secs: i64,
        mtime_nanos: i32,
    ) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
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

    pub fn read(mut self, offset: u64, size: u32) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!(
            "Reading file: {}. data_dir={}",
            self.path, self.local_data_dir
        );
        let path = Path::new(&self.local_data_dir).join(&self.path);
        let file = File::open(&path).map_err(into_error_code)?;

        let mut contents = vec![0; size as usize];
        let bytes_read = file
            .read_at(&mut contents, offset)
            .map_err(into_error_code)?;
        contents.truncate(bytes_read);

        let data_offset = self.response_buffer.create_vector_direct(&contents);
        let mut response_builder = ReadResponseBuilder::new(&mut self.response_buffer);
        response_builder.add_data(data_offset);
        let response_offset = response_builder.finish().as_union_value();

        return Ok((
            self.response_buffer,
            ResponseType::ReadResponse,
            response_offset,
        ));
    }

    pub fn unlink(self) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!("Deleting file");
        let local_path = self.to_local_path(&self.path);
        fs::remove_file(local_path).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }
}
