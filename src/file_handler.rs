use std::ffi::CString;
use std::fs::File;
use std::io::Write;
use std::os::linux::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::{fs, io};

use filetime::FileTime;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use log::info;
use log::warn;

use crate::data_storage::{DataStorage, BLOCK_SIZE};
use crate::generated::*;
use crate::metadata_storage::MetadataStorage;
use crate::peer_client::PeerClient;
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, finalize_response, into_error_code, ResultResponse};
use futures::future::result;
use futures::Future;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

fn to_read_response<'a>(mut builder: FlatBufferBuilder<'a>, data: &[u8]) -> ResultResponse<'a> {
    let data_offset = builder.create_vector_direct(data);
    let mut response_builder = ReadResponseBuilder::new(&mut builder);
    response_builder.add_data(data_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::ReadResponse, response_offset));
}

fn to_write_response(mut builder: FlatBufferBuilder, length: u32) -> ResultResponse {
    let mut response_builder = WrittenResponseBuilder::new(&mut builder);
    response_builder.add_bytes_written(length);
    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::WrittenResponse, offset));
}

pub fn file_request_handler<'a, 'b>(
    request: GenericRequest<'a>,
    data_storage: &DataStorage,
    metadata_storage: &MetadataStorage,
    context: &LocalContext,
    builder: FlatBufferBuilder<'b>,
) -> impl Future<Item = FlatBufferBuilder<'b>, Error = ErrorCode> {
    let response: Box<
        Future<
                Item = (
                    FlatBufferBuilder<'b>,
                    ResponseType,
                    WIPOffset<UnionWIPOffset>,
                ),
                Error = ErrorCode,
            > + Send,
    >;

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            response = Box::new(fsck(context, builder));
        }
        RequestType::FilesystemChecksumRequest => {
            response = Box::new(result(checksum_request(context, builder)));
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            let read_result = data_storage.read(
                read_request.path(),
                read_request.offset(),
                read_request.read_size(),
            );
            response =
                Box::new(read_result.map(move |data| to_read_response(builder, &data).unwrap()))
        }
        RequestType::ReadRawRequest => {
            let read_request = request.request_as_read_raw_request().unwrap();
            let read_result = data_storage.read_raw(
                read_request.path(),
                read_request.offset(),
                read_request.read_size(),
            );
            response = Box::new(result(
                read_result
                    .map(move |data| to_read_response(builder, &data).unwrap())
                    .map_err(into_error_code),
            ));
        }
        RequestType::HardlinkRequest => {
            let hardlink_request = request.request_as_hardlink_request().unwrap();
            let file = FileRequestHandler::new(
                hardlink_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            metadata_storage.hardlink(
                &file.path,
                hardlink_request.new_path().trim_start_matches('/'),
            );
            response = Box::new(result(file.hardlink(&hardlink_request.new_path())));
        }
        RequestType::RenameRequest => {
            let rename_request = request.request_as_rename_request().unwrap();
            let file = FileRequestHandler::new(
                rename_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            metadata_storage.rename(
                &file.path,
                rename_request.new_path().trim_start_matches('/'),
            );
            response = Box::new(result(file.rename(&rename_request.new_path())));
        }
        RequestType::ChmodRequest => {
            let chmod_request = request.request_as_chmod_request().unwrap();
            let file = FileRequestHandler::new(
                chmod_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.chmod(chmod_request.mode())));
        }
        RequestType::TruncateRequest => {
            let truncate_request = request.request_as_truncate_request().unwrap();
            let file = FileRequestHandler::new(
                truncate_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            metadata_storage.truncate(&file.path, truncate_request.new_length());
            response = Box::new(result(file.truncate(truncate_request.new_length())));
        }
        RequestType::UnlinkRequest => {
            let unlink_request = request.request_as_unlink_request().unwrap();
            let file = FileRequestHandler::new(
                unlink_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            metadata_storage.unlink(&file.path);
            response = Box::new(result(file.unlink()));
        }
        RequestType::RmdirRequest => {
            let rmdir_request = request.request_as_rmdir_request().unwrap();
            metadata_storage.rmdir(rmdir_request.path());
            let rmdir_result = data_storage
                .rmdir(rmdir_request.path())
                .map(|_| empty_response(builder).unwrap());
            response = Box::new(result(rmdir_result));
        }
        RequestType::WriteRequest => {
            let write_request = request.request_as_write_request().unwrap();
            metadata_storage.write(
                write_request.path().trim_start_matches('/'),
                write_request.offset(),
                write_request.data().len() as u32,
            );
            let write_result = data_storage.write_local_blocks(
                write_request.path(),
                write_request.offset(),
                write_request.data(),
            );
            // Reply with the total requested write size, since that's what the FUSE client is expecting, even though this node only wrote some of the bytes
            let total_bytes = write_request.data().len() as u32;
            response = Box::new(result(
                write_result
                    .map(move |_| to_write_response(builder, total_bytes).unwrap())
                    .map_err(into_error_code),
            ));
        }
        RequestType::UtimensRequest => {
            let utimens_request = request.request_as_utimens_request().unwrap();
            let file = FileRequestHandler::new(
                utimens_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.utimens(
                utimens_request.atime().map(Timestamp::seconds).unwrap_or(0),
                utimens_request.atime().map(Timestamp::nanos).unwrap_or(0),
                utimens_request.mtime().map(Timestamp::seconds).unwrap_or(0),
                utimens_request.mtime().map(Timestamp::nanos).unwrap_or(0),
            )));
        }
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let file = FileRequestHandler::new(
                readdir_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.readdir()));
        }
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let file = FileRequestHandler::new(
                getattr_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.getattr(metadata_storage)));
        }
        RequestType::MkdirRequest => {
            let mkdir_request = request.request_as_mkdir_request().unwrap();
            let file = FileRequestHandler::new(
                mkdir_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            metadata_storage.mkdir(&file.path);
            response = Box::new(result(file.mkdir(mkdir_request.mode()).map(|builder| {
                let file = FileRequestHandler::new(
                    mkdir_request.path().to_string(),
                    context.data_dir.clone(),
                    builder,
                );
                // TODO: probably possible to hit a distributed race here
                file.getattr(metadata_storage)
                    .expect("getattr failed on newly created file")
            })));
        }
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::GetLeaderRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }

    response.map(|(mut builder, response_type, response_offset)| {
        finalize_response(&mut builder, response_type, response_offset);
        builder
    })
}

// Handles one request/response
struct FileRequestHandler<'a> {
    path: String,
    local_data_dir: String,
    response_buffer: FlatBufferBuilder<'a>,
}

impl<'a> FileRequestHandler<'a> {
    fn new(
        path: String,
        local_data_dir: String,
        builder: FlatBufferBuilder<'a>,
    ) -> FileRequestHandler<'a> {
        FileRequestHandler {
            // XXX: hack
            path: path.trim_start_matches('/').to_string(),
            local_data_dir,
            response_buffer: builder,
        }
    }

    fn to_local_path(&self, path: &str) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    fn truncate(self, new_length: u64) -> ResultResponse<'a> {
        let local_path = self.to_local_path(&self.path);
        let file = File::create(&local_path).expect("Couldn't create file");
        file.set_len(new_length).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }

    fn mkdir(self, _mode: u16) -> Result<FlatBufferBuilder<'a>, ErrorCode> {
        assert_ne!(self.path.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.path);
        fs::create_dir(path).map_err(into_error_code)?;

        // TODO set the mode

        return Ok(self.response_buffer);
    }

    fn readdir(mut self) -> ResultResponse<'a> {
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

    fn getattr(mut self, metadata_storage: &MetadataStorage) -> ResultResponse<'a> {
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

    fn utimens(
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

    fn chmod(self, mode: u32) -> ResultResponse<'a> {
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

    fn hardlink(mut self, new_path: &str) -> ResultResponse<'a> {
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

    fn rename(self, new_path: &str) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!("Renaming file: {} to {}", self.path, new_path);
        let local_path = self.to_local_path(&self.path);
        let local_new_path = self.to_local_path(new_path);
        fs::rename(local_path, local_new_path).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }

    fn unlink(self) -> ResultResponse<'a> {
        assert_ne!(self.path.len(), 0);

        info!("Deleting file");
        let local_path = self.to_local_path(&self.path);
        fs::remove_file(local_path).map_err(into_error_code)?;

        return empty_response(self.response_buffer);
    }
}

fn checksum(data_dir: &str) -> io::Result<Vec<u8>> {
    let mut hasher = Sha256::new();
    for entry in WalkDir::new(data_dir).sort_by(|a, b| a.file_name().cmp(b.file_name())) {
        let entry = entry?;
        if entry.file_type().is_file() {
            // TODO hash the data and file attributes too
            let path_bytes = entry
                .path()
                .to_str()
                .unwrap()
                .trim_start_matches(data_dir)
                .as_bytes();
            hasher.write_all(path_bytes).unwrap();
        }
        // TODO handle other file types
    }
    return Ok(hasher.result().to_vec());
}

fn fsck<'a>(
    context: &LocalContext,
    mut builder: FlatBufferBuilder<'a>,
) -> impl Future<
    Item = (
        FlatBufferBuilder<'a>,
        ResponseType,
        WIPOffset<UnionWIPOffset>,
    ),
    Error = ErrorCode,
> {
    let future_checksum = result(checksum(&context.data_dir).map_err(into_error_code));
    let mut peer_futures = vec![];
    for peer in context.peers.iter() {
        let client = PeerClient::new(*peer);
        peer_futures.push(client.filesystem_checksum().map_err(into_error_code));
    }

    futures::future::join_all(peer_futures)
        .join(future_checksum)
        .map(move |(peer_checksums, checksum)| {
            for peer_checksum in peer_checksums {
                if checksum != peer_checksum {
                    let args = ErrorResponseArgs {
                        error_code: ErrorCode::Corrupted,
                    };
                    let response_offset =
                        ErrorResponse::create(&mut builder, &args).as_union_value();
                    return (builder, ResponseType::ErrorResponse, response_offset);
                }
            }

            return empty_response(builder).unwrap();
        })
}

fn checksum_request<'a>(
    local_context: &LocalContext,
    mut builder: FlatBufferBuilder<'a>,
) -> ResultResponse<'a> {
    let checksum = checksum(&local_context.data_dir).map_err(|_| ErrorCode::Uncategorized)?;
    let data_offset = builder.create_vector_direct(&checksum);
    let mut response_builder = ReadResponseBuilder::new(&mut builder);
    response_builder.add_data(data_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::ReadResponse, response_offset));
}
