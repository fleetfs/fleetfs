use std::ffi::CString;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::net::SocketAddr;
use std::os::linux::fs::MetadataExt;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

use filetime::FileTime;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use log::info;
use log::warn;

use crate::client::NodeClient;
use crate::generated::*;
use crate::utils::{empty_response, ResultResponse};

// Handles one request/response
pub struct DistributedFileResponder<'a: 'b, 'b> {
    path: String,
    local_data_dir: String,
    peers: Vec<NodeClient<'a>>,
    response_buffer: &'b mut FlatBufferBuilder<'a>,
}

impl<'a: 'b, 'b> DistributedFileResponder<'a, 'b> {
    pub fn new(
        path: String,
        local_data_dir: String,
        peers: &[SocketAddr],
        builder: &'b mut FlatBufferBuilder<'a>,
    ) -> DistributedFileResponder<'a, 'b> {
        DistributedFileResponder {
            // XXX: hack
            path: path.trim_start_matches('/').to_string(),
            local_data_dir,
            peers: peers.iter().map(|peer| NodeClient::new(*peer)).collect(),
            response_buffer: builder,
        }
    }

    fn to_local_path(&self, path: &str) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    pub fn truncate(self, new_length: u64, forward: bool) -> ResultResponse {
        let local_path = self.to_local_path(&self.path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.truncate(&self.path, new_length, false).unwrap();
            }
        }
        let file = File::create(&local_path).expect("Couldn't create file");
        file.set_len(new_length)?;

        return empty_response(self.response_buffer);
    }

    pub fn write(self, offset: u64, data: &[u8], forward: bool) -> ResultResponse {
        let local_path = self.to_local_path(&self.path);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&local_path)?;

        file.seek(SeekFrom::Start(offset))?;

        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.write(&self.path, data, offset, false).unwrap();
            }
        }

        file.write_all(data)?;
        let mut response_builder = WrittenResponseBuilder::new(self.response_buffer);
        response_builder.add_bytes_written(data.len() as u32);
        let offset = response_builder.finish().as_union_value();
        return Ok((ResponseType::WrittenResponse, offset));
    }

    pub fn mkdir(self, _mode: u16, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.path.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.path);
        fs::create_dir(path)?;

        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.mkdir(&self.path, _mode, false).unwrap();
            }
        }
        // TODO set the mode

        return Ok(());
    }

    pub fn readdir(self) -> ResultResponse {
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
                self.response_buffer,
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
        let mut builder = DirectoryListingResponseBuilder::new(self.response_buffer);
        builder.add_entries(entries);

        return Ok((
            ResponseType::DirectoryListingResponse,
            builder.finish().as_union_value(),
        ));
    }

    pub fn getattr(self) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.path);
        let metadata = fs::metadata(path)?;

        let mut builder = FileMetadataResponseBuilder::new(self.response_buffer);
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

        return Ok((
            ResponseType::FileMetadataResponse,
            builder.finish().as_union_value(),
        ));
    }

    pub fn utimens(
        self,
        atime_secs: i64,
        atime_nanos: i32,
        mtime_secs: i64,
        mtime_nanos: i32,
        forward: bool,
    ) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.utimens(
                    &self.path,
                    atime_secs,
                    atime_nanos,
                    mtime_secs,
                    mtime_nanos,
                    false,
                )
                .unwrap();
            }
        }
        filetime::set_file_times(
            local_path,
            FileTime::from_unix_time(atime_secs, atime_nanos as u32),
            FileTime::from_unix_time(mtime_secs, mtime_nanos as u32),
        )?;

        return empty_response(self.response_buffer);
    }

    pub fn chmod(self, mode: u32, forward: bool) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.chmod(&self.path, mode, false).unwrap();
            }
        }
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
            return Err(std::io::Error::from(std::io::ErrorKind::Other));
        }
    }

    pub fn hardlink(self, new_path: &str, forward: bool) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        info!("Hardlinking file: {} to {}", self.path, new_path);
        let local_path = self.to_local_path(&self.path);
        let local_new_path = self.to_local_path(new_path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.hardlink(&self.path, new_path, false).unwrap();
            }
        }
        // TODO error handling
        fs::hard_link(&local_path, &local_new_path).expect("hardlink failed");
        let metadata = fs::metadata(local_new_path)?;

        let mut builder = FileMetadataResponseBuilder::new(self.response_buffer);
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

        return Ok((
            ResponseType::FileMetadataResponse,
            builder.finish().as_union_value(),
        ));
    }

    pub fn rename(self, new_path: &str, forward: bool) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        info!("Renaming file: {} to {}", self.path, new_path);
        let local_path = self.to_local_path(&self.path);
        let local_new_path = self.to_local_path(new_path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.rename(&self.path, new_path, false).unwrap();
            }
        }
        fs::rename(local_path, local_new_path)?;

        return empty_response(self.response_buffer);
    }

    pub fn read(self, offset: u64, size: u32) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        info!(
            "Reading file: {}. data_dir={}",
            self.path, self.local_data_dir
        );
        let path = Path::new(&self.local_data_dir).join(&self.path);
        let file = File::open(&path)?;

        let mut contents = vec![0; size as usize];
        let bytes_read = file.read_at(&mut contents, offset)?;
        contents.truncate(bytes_read);

        let data_offset = self.response_buffer.create_vector_direct(&contents);
        let mut response_builder = ReadResponseBuilder::new(self.response_buffer);
        response_builder.add_data(data_offset);
        let response_offset = response_builder.finish().as_union_value();

        return Ok((ResponseType::ReadResponse, response_offset));
    }

    pub fn unlink(self, forward: bool) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        info!("Deleting file");
        let local_path = self.to_local_path(&self.path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.unlink(&self.path, false).unwrap();
            }
        }
        fs::remove_file(local_path)?;

        return empty_response(self.response_buffer);
    }
}
