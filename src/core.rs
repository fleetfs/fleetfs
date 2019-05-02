use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

use flatbuffers::{FlatBufferBuilder, WIPOffset, UnionWIPOffset};
use futures::Stream;
use log::info;
use log::warn;
use tokio::codec::length_delimited;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::generated::*;
use std::os::linux::fs::MetadataExt;
use std::net::SocketAddr;
use filetime::FileTime;
use std::ffi::CString;
use crate::client::NodeClient;


struct DistributedFile {
    filename: String,
    local_data_dir: String,
    peers: Vec<NodeClient>
}

impl DistributedFile {
    fn new(filename: String, local_data_dir: String, peers: &Vec<SocketAddr>) -> DistributedFile {
        DistributedFile {
            // XXX: hack
            filename: filename.trim_start_matches('/').to_string(),
            local_data_dir,
            peers: peers.iter().map(|peer| NodeClient::new(peer)).collect()
        }
    }

    fn to_local_path(&self, path: &String) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    fn truncate(self, new_length: u64, forward: bool) -> Result<(), std::io::Error> {
        let local_path = self.to_local_path(&self.filename);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.truncate(&self.filename, new_length, false).unwrap();
            }
        }
        let file = File::create(&local_path).expect("Couldn't create file");
        return file.set_len(new_length);
    }

    fn write(self, offset: u64, data: &[u8], forward: bool) -> Result<u32, std::io::Error> {
        let local_path = self.to_local_path(&self.filename);
        let mut file = OpenOptions::new().write(true).create(true).open(&local_path)?;

        file.seek(SeekFrom::Start(offset))?;

        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.write(&self.filename, data, offset,false).unwrap();
            }
        }

        file.write_all(data)?;
        return Ok(data.len() as u32);
    }

    fn mkdir(self, _mode: u16, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.filename);
        fs::create_dir(path)?;

        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.mkdir(&self.filename, _mode, false).unwrap();
            }
        }
        // TODO set the mode

        return Ok(());
    }

    fn readdir(self, buffer: &mut FlatBufferBuilder) -> Result<WIPOffset<UnionWIPOffset>, std::io::Error> {
        let path = Path::new(&self.local_data_dir).join(&self.filename);

        let mut entries = vec![];
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let filename = entry.file_name().clone().to_str().unwrap().to_string();
            let file_type = if entry.file_type().unwrap().is_file() {
                FileType::File
            }
            else if entry.file_type().unwrap().is_dir() {
                FileType::Directory
            }
            else {
                unimplemented!()
            };
            let filename = buffer.create_string(&filename);
            let directory_entry = DirectoryEntry::create(buffer, &DirectoryEntryArgs {filename: Some(filename), kind: file_type});
            entries.push(directory_entry);
        }
        buffer.start_vector::<WIPOffset<DirectoryEntry>>(entries.len());
        for &directory_entry in entries.iter() {
            buffer.push(directory_entry);
        }
        let entries = buffer.end_vector(entries.len());
        let mut builder = DirectoryListingResponseBuilder::new(buffer);
        builder.add_entries(entries);

        return Ok(builder.finish().as_union_value());
    }

    fn getattr(self, buffer: &mut FlatBufferBuilder) -> Result<WIPOffset<UnionWIPOffset>, std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.filename);
        let metadata = fs::metadata(path)?;

        let mut builder = FileMetadataResponseBuilder::new(buffer);
        builder.add_size_bytes(metadata.len());
        builder.add_size_blocks(metadata.st_blocks());
        let atime = Timestamp::new(metadata.st_atime(), metadata.st_atime_nsec() as i32);
        builder.add_last_access_time(&atime);
        let mtime = Timestamp::new(metadata.st_mtime(), metadata.st_mtime_nsec() as i32);
        builder.add_last_modified_time(&mtime);
        let ctime = Timestamp::new(metadata.st_ctime(), metadata.st_ctime_nsec() as i32);
        builder.add_last_metadata_modified_time(&ctime);
        if metadata.is_file() {
            builder.add_kind(FileType::File);
        }
        else if metadata.is_dir() {
            builder.add_kind(FileType::Directory);
        }
        else {
            unimplemented!();
        }
        // TODO: permissions
        builder.add_mode(metadata.st_mode() as u16);
        builder.add_hard_links(metadata.st_nlink() as u32);
        builder.add_user_id(metadata.st_uid());
        builder.add_group_id(metadata.st_gid());
        builder.add_device_id(metadata.st_rdev() as u32);

        return Ok(builder.finish().as_union_value());
    }

    fn utimens(self, atime_secs: i64, atime_nanos: i32, mtime_secs: i64, mtime_nanos: i32, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        let local_path = self.to_local_path(&self.filename);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.utimens(&self.filename, atime_secs, atime_nanos, mtime_secs, mtime_nanos, false).unwrap();
            }
        }
        return filetime::set_file_times(local_path,
                                        FileTime::from_unix_time(atime_secs, atime_nanos as u32),
                                        FileTime::from_unix_time(mtime_secs, mtime_nanos as u32));
    }

    fn chmod(self, mode: u32, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        let local_path = self.to_local_path(&self.filename);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.chmod(&self.filename, mode, false).unwrap();
            }
        }
        let c_path = CString::new(local_path.to_str().unwrap().as_bytes()).expect("CString creation failed");
        let exit_code;
        unsafe {
            exit_code = libc::chmod(c_path.as_ptr(), mode)
        }
        if exit_code == libc::EXIT_SUCCESS {
            return Ok(());
        }
        else {
            warn!("chmod failed on {:?}, {} with error {:?}", local_path, mode, exit_code);
            return Err(std::io::Error::from(std::io::ErrorKind::Other));
        }
    }

    fn hardlink(self, new_path: &String, buffer: &mut FlatBufferBuilder, forward: bool) -> Result<WIPOffset<UnionWIPOffset>, std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        info!("Hardlinking file: {} to {}", self.filename, new_path);
        let local_path = self.to_local_path(&self.filename);
        let local_new_path = self.to_local_path(new_path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.hardlink(&self.filename, new_path, false).unwrap();
            }
        }
        // TODO error handling
        fs::hard_link(&local_path, &local_new_path).expect("hardlink failed");
        let metadata = fs::metadata(local_new_path)?;

        let mut builder = FileMetadataResponseBuilder::new(buffer);
        builder.add_size_bytes(metadata.len());
        builder.add_size_blocks(metadata.st_blocks());
        let atime = Timestamp::new(metadata.st_atime(), metadata.st_atime_nsec() as i32);
        builder.add_last_access_time(&atime);
        let mtime = Timestamp::new(metadata.st_mtime(), metadata.st_mtime_nsec() as i32);
        builder.add_last_modified_time(&mtime);
        let ctime = Timestamp::new(metadata.st_ctime(), metadata.st_ctime_nsec() as i32);
        builder.add_last_metadata_modified_time(&ctime);
        if metadata.is_file() {
            builder.add_kind(FileType::File);
        }
        else if metadata.is_dir() {
            builder.add_kind(FileType::Directory);
        }
        else {
            unimplemented!();
        }
        // TODO: permissions
        builder.add_mode(metadata.st_mode() as u16);
        builder.add_hard_links(metadata.st_nlink() as u32);
        builder.add_user_id(metadata.st_uid());
        builder.add_group_id(metadata.st_gid());
        builder.add_device_id(metadata.st_rdev() as u32);

        return Ok(builder.finish().as_union_value());
    }

    fn rename(self, new_path: &String, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        info!("Renaming file: {} to {}", self.filename, new_path);
        let local_path = self.to_local_path(&self.filename);
        let local_new_path = self.to_local_path(new_path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.rename(&self.filename, new_path, false).unwrap();
            }
        }
        return fs::rename(local_path, local_new_path);
    }

    fn read(self, offset: u64, size: u32) -> Result<Vec<u8>, std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        info!("Reading file: {}. data_dir={}", self.filename, self.local_data_dir);
        let path = Path::new(&self.local_data_dir).join(&self.filename);
        let file = File::open(&path)?;

        let mut contents = vec![0; size as usize];
        let bytes_read = file.read_at(&mut contents, offset)?;
        contents.truncate(bytes_read);

        return Ok(contents);
    }

    fn unlink(self, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        info!("Deleting file");
        let local_path = self.to_local_path(&self.filename);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.unlink(&self.filename, false).unwrap();
            }
        }
        return fs::remove_file(local_path);
    }

}

fn handler<'a, 'b>(request: GenericRequest<'a>, context: &LocalContext) -> FlatBufferBuilder<'b> {
    let mut builder = FlatBufferBuilder::new();
    let response_type;
    let response_offset;

    match request.request_type() {
        RequestType::ReadRequest => {
            response_type = ResponseType::ReadResponse;
            let read_request = request.request_as_read_request().unwrap();
            let file = DistributedFile::new(read_request.filename().to_string(), context.data_dir.clone(), &context.peers);
            let data = file.read(read_request.offset(), read_request.read_size());
            let data_offset = builder.create_vector_direct(&data.unwrap());
            let mut response_builder = ReadResponseBuilder::new(&mut builder);
            response_builder.add_data(data_offset);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::HardlinkRequest => {
            let hardlink_request = request.request_as_hardlink_request().unwrap();
            let file = DistributedFile::new(hardlink_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            let offset = file.hardlink(&hardlink_request.new_path().to_string(), &mut builder, hardlink_request.forward()).unwrap();
            response_type = ResponseType::FileMetadataResponse;
            response_offset = offset;
        },
        RequestType::RenameRequest => {
            response_type = ResponseType::EmptyResponse;
            let rename_request = request.request_as_rename_request().unwrap();
            let file = DistributedFile::new(rename_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            file.rename(&rename_request.new_path().to_string(), rename_request.forward()).unwrap();
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::ChmodRequest => {
            response_type = ResponseType::EmptyResponse;
            let chmod_request = request.request_as_chmod_request().unwrap();
            let file = DistributedFile::new(chmod_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            file.chmod(chmod_request.mode(), chmod_request.forward()).unwrap();
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::TruncateRequest => {
            response_type = ResponseType::EmptyResponse;
            let truncate_request = request.request_as_truncate_request().unwrap();
            let file = DistributedFile::new(truncate_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            file.truncate(truncate_request.new_length(), truncate_request.forward()).unwrap();
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::UnlinkRequest => {
            response_type = ResponseType::EmptyResponse;
            let unlink_request = request.request_as_unlink_request().unwrap();
            let file = DistributedFile::new(unlink_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            file.unlink(unlink_request.forward()).unwrap();
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::WriteRequest => {
            response_type = ResponseType::WrittenResponse;
            let write_request = request.request_as_write_request().unwrap();
            let file = DistributedFile::new(write_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            let written = file.write(write_request.offset(), write_request.data(), write_request.forward()).unwrap();
            let mut response_builder = WrittenResponseBuilder::new(&mut builder);
            response_builder.add_bytes_written(written);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::UtimensRequest => {
            response_type = ResponseType::EmptyResponse;
            let utimens_request = request.request_as_utimens_request().unwrap();
            let file = DistributedFile::new(utimens_request.path().to_string(), context.data_dir.clone(), &context.peers);
            // TODO handle errors
            file.utimens(utimens_request.atime().map(|x| x.seconds()).unwrap_or(0),
                        utimens_request.atime().map(|x| x.nanos()).unwrap_or(0),
                         utimens_request.mtime().map(|x| x.seconds()).unwrap_or(0),
                         utimens_request.mtime().map(|x| x.nanos()).unwrap_or(0),
                         utimens_request.forward()).unwrap();
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let file = DistributedFile::new(readdir_request.path().to_string(), context.data_dir.clone(), &context.peers);
            match file.readdir(&mut builder) {
                Ok(offset) => {
                    response_type = ResponseType::DirectoryListingResponse;
                    response_offset = offset;
                },
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        response_type = ResponseType::ErrorResponse;
                        let args = ErrorResponseArgs {error_code: ErrorCode::DoesNotExist};
                        response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
                    }
                    else {
                        // TODO
                        unimplemented!();
                    }
                }
            }
        },
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let file = DistributedFile::new(getattr_request.filename().to_string(), context.data_dir.clone(), &context.peers);
            match file.getattr(&mut builder) {
                Ok(offset) => {
                    response_type = ResponseType::FileMetadataResponse;
                    response_offset = offset;
                },
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        response_type = ResponseType::ErrorResponse;
                        let args = ErrorResponseArgs {error_code: ErrorCode::DoesNotExist};
                        response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
                    }
                    else {
                        // TODO
                        unimplemented!();
                    }
                }
            }
        },
        RequestType::MkdirRequest => {
            let mkdir_request = request.request_as_mkdir_request().unwrap();
            let file = DistributedFile::new(mkdir_request.path().to_string(), context.data_dir.clone(), &context.peers);
            match file.mkdir(mkdir_request.mode(), mkdir_request.forward()) {
                Ok(_) => {
                    let file = DistributedFile::new(mkdir_request.path().to_string(), context.data_dir.clone(), &context.peers);
                    match file.getattr(&mut builder) {
                        Ok(offset) => {
                            response_type = ResponseType::FileMetadataResponse;
                            response_offset = offset;
                        },
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::NotFound {
                                response_type = ResponseType::ErrorResponse;
                                let args = ErrorResponseArgs {error_code: ErrorCode::DoesNotExist};
                                response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
                            }
                            else {
                                // TODO
                                unimplemented!();
                            }
                        }
                    }
                },
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        response_type = ResponseType::ErrorResponse;
                        let args = ErrorResponseArgs {error_code: ErrorCode::DoesNotExist};
                        response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
                    }
                    else {
                        // TODO
                        unimplemented!();
                    }
                }
            }
        },
        RequestType::NONE => unreachable!()
    }

    let mut generic_response_builder = GenericResponseBuilder::new(&mut builder);
    generic_response_builder.add_response_type(response_type);
    generic_response_builder.add_response(response_offset);
    let final_response_offset = generic_response_builder.finish();
    builder.finish_size_prefixed(final_response_offset, None);
    return builder;
}

#[derive(Clone)]
struct LocalContext {
    data_dir: String,
    peers: Vec<SocketAddr>
}

impl LocalContext {
    pub fn new(data_dir: String, peers: Vec<SocketAddr>) -> LocalContext {
        LocalContext {
            data_dir,
            peers
        }
    }
}

struct WritableFlatBuffer<'a> {
    buffer: FlatBufferBuilder<'a>
}

impl <'a> AsRef<[u8]> for WritableFlatBuffer<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.finished_data()
    }
}

pub struct Node {
    context: LocalContext,
    port: u16
}

impl Node {
    pub fn new(data_dir: String, port: u16, peers: Vec<SocketAddr>) -> Node {
        Node {
            context: LocalContext::new(data_dir, peers),
            port: port
        }
    }

    pub fn run(self) {
        match fs::create_dir_all(&self.context.data_dir) {
            Err(why) => panic!("Couldn't create storage dir: {}", why.description()),
            Ok(_) => ()

        };

        let address = ([127, 0, 0, 1], self.port).into();
        let listener = TcpListener::bind(&address)
            .expect("unable to bind API listener");

        let context = self.context.clone();
        let server = listener.incoming()
            .map_err(|e| eprintln!("accept connection failed = {:?}", e))
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let reader = length_delimited::Builder::new()
                    .little_endian()
                    .new_read(reader);

                let cloned = context.clone();
                let conn = reader.fold(writer, move |writer, frame| {
                    let request = get_root_as_generic_request(&frame);
                    let response_builder = handler(request, &cloned);
                    let writable = WritableFlatBuffer {buffer: response_builder};
                    tokio::io::write_all(writer, writable).map(|(writer, _)| writer)
                });

                tokio::spawn(conn.map(|_| ()).map_err(|_| ()))
            });

        tokio::runtime::run(server.map(|_| ()));
    }
}
