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
use crate::local_storage::LocalStorage;

type ResultResponse = Result<(ResponseType, WIPOffset<UnionWIPOffset>), std::io::Error>;

fn empty_response(buffer: &mut FlatBufferBuilder) -> ResultResponse {
    let response_builder = EmptyResponseBuilder::new(buffer);
    let offset = response_builder.finish().as_union_value();
    return Ok((ResponseType::EmptyResponse, offset));
}


// Handles one request/response
struct DistributedFileResponder<'a: 'b, 'b>  {
    path: String,
    local_data_dir: String,
    peers: Vec<NodeClient<'a>>,
    response_buffer: &'b mut FlatBufferBuilder<'a>
}

impl <'a: 'b, 'b> DistributedFileResponder<'a, 'b> {
    fn new(path: String, local_data_dir: String, peers: &Vec<SocketAddr>, builder: &'b mut FlatBufferBuilder<'a>) -> DistributedFileResponder<'a, 'b> {
        DistributedFileResponder {
            // XXX: hack
            path: path.trim_start_matches('/').to_string(),
            local_data_dir,
            peers: peers.iter().map(|peer| NodeClient::new(*peer)).collect(),
            response_buffer: builder
        }
    }

    fn to_local_path(&self, path: &str) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    fn truncate(self, new_length: u64, forward: bool) -> ResultResponse {
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

    fn write(self, offset: u64, data: &[u8], forward: bool) -> ResultResponse {
        let local_path = self.to_local_path(&self.path);
        let mut file = OpenOptions::new().write(true).create(true).open(&local_path)?;

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

    fn mkdir(self, _mode: u16, forward: bool) -> Result<(), std::io::Error> {
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

    fn readdir(self) -> ResultResponse {
        let path = Path::new(&self.local_data_dir).join(&self.path);

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
            let path = self.response_buffer.create_string(&filename);
            let directory_entry = DirectoryEntry::create(self.response_buffer, &DirectoryEntryArgs {path: Some(path), kind: file_type});
            entries.push(directory_entry);
        }
        self.response_buffer.start_vector::<WIPOffset<DirectoryEntry>>(entries.len());
        for &directory_entry in entries.iter() {
            self.response_buffer.push(directory_entry);
        }
        let entries = self.response_buffer.end_vector(entries.len());
        let mut builder = DirectoryListingResponseBuilder::new(self.response_buffer);
        builder.add_entries(entries);

        return Ok((ResponseType::DirectoryListingResponse, builder.finish().as_union_value()));
    }

    fn getattr(self) -> ResultResponse {
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
            builder.add_kind(FileType::File);
        }
        else if metadata.is_dir() {
            builder.add_kind(FileType::Directory);
        }
        else {
            unimplemented!();
        }
        builder.add_mode(metadata.st_mode() as u16);
        builder.add_hard_links(metadata.st_nlink() as u32);
        builder.add_user_id(metadata.st_uid());
        builder.add_group_id(metadata.st_gid());
        builder.add_device_id(metadata.st_rdev() as u32);

        return Ok((ResponseType::FileMetadataResponse, builder.finish().as_union_value()));
    }

    fn utimens(self, atime_secs: i64, atime_nanos: i32, mtime_secs: i64, mtime_nanos: i32, forward: bool) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.utimens(&self.path, atime_secs, atime_nanos, mtime_secs, mtime_nanos, false).unwrap();
            }
        }
        filetime::set_file_times(local_path,
                                 FileTime::from_unix_time(atime_secs, atime_nanos as u32),
                                 FileTime::from_unix_time(mtime_secs, mtime_nanos as u32))?;

        return empty_response(self.response_buffer);
    }

    fn chmod(self, mode: u32, forward: bool) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        let local_path = self.to_local_path(&self.path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.chmod(&self.path, mode, false).unwrap();
            }
        }
        let c_path = CString::new(local_path.to_str().unwrap().as_bytes()).expect("CString creation failed");
        let exit_code;
        unsafe {
            exit_code = libc::chmod(c_path.as_ptr(), mode)
        }
        if exit_code == libc::EXIT_SUCCESS {
            return empty_response(self.response_buffer);
        }
        else {
            warn!("chmod failed on {:?}, {} with error {:?}", local_path, mode, exit_code);
            return Err(std::io::Error::from(std::io::ErrorKind::Other));
        }
    }

    fn hardlink(self, new_path: &str, forward: bool) -> ResultResponse {
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
            builder.add_kind(FileType::File);
        }
        else if metadata.is_dir() {
            builder.add_kind(FileType::Directory);
        }
        else {
            unimplemented!();
        }
        builder.add_mode(metadata.st_mode() as u16);
        builder.add_hard_links(metadata.st_nlink() as u32);
        builder.add_user_id(metadata.st_uid());
        builder.add_group_id(metadata.st_gid());
        builder.add_device_id(metadata.st_rdev() as u32);

        return Ok((ResponseType::FileMetadataResponse, builder.finish().as_union_value()));
    }

    fn rename(self, new_path: &str, forward: bool) -> ResultResponse {
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

    fn read(self, offset: u64, size: u32) -> ResultResponse {
        assert_ne!(self.path.len(), 0);

        info!("Reading file: {}. data_dir={}", self.path, self.local_data_dir);
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

    fn unlink(self, forward: bool) -> ResultResponse {
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

fn fsck(context: &LocalContext) -> Result<(), ErrorCode> {
    let local_storage = LocalStorage::new(&context.data_dir);
    let checksum = local_storage.checksum().map_err(|_| ErrorCode::Uncategorized)?;
    for &peer in context.peers.iter() {
        let client = NodeClient::new(peer);
        let peer_checksum = client.filesystem_checksum()?;
        if checksum != peer_checksum {
            return Err(ErrorCode::Corrupted);
        }
    }
    return Ok(());
}

fn checksum_request(data_dir: &str, builder: &mut FlatBufferBuilder) -> ResultResponse {
    let local_storage = LocalStorage::new(data_dir);
    let checksum = local_storage.checksum()?;
    let data_offset = builder.create_vector_direct(&checksum);
    let mut response_builder = ReadResponseBuilder::new(builder);
    response_builder.add_data(data_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((ResponseType::ReadResponse, response_offset));
}

fn handler<'a>(request: GenericRequest<'a>, context: &LocalContext, builder: &mut FlatBufferBuilder) {
    let response;
    let mut known_error_code = None;

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            response = match fsck(context) {
                Ok(_) => empty_response(builder),
                Err(error_code) => {
                    known_error_code = Some(error_code);
                    Err(std::io::Error::from(std::io::ErrorKind::Other))
                }
            };
        },
        RequestType::FilesystemChecksumRequest => {
            response = checksum_request(&context.data_dir, builder);
        },
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            let file = DistributedFileResponder::new(read_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.read(read_request.offset(), read_request.read_size());
        },
        RequestType::HardlinkRequest => {
            let hardlink_request = request.request_as_hardlink_request().unwrap();
            let file = DistributedFileResponder::new(hardlink_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.hardlink(&hardlink_request.new_path(), hardlink_request.forward());
        },
        RequestType::RenameRequest => {
            let rename_request = request.request_as_rename_request().unwrap();
            let file = DistributedFileResponder::new(rename_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.rename(&rename_request.new_path(), rename_request.forward());
        },
        RequestType::ChmodRequest => {
            let chmod_request = request.request_as_chmod_request().unwrap();
            let file = DistributedFileResponder::new(chmod_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.chmod(chmod_request.mode(), chmod_request.forward());
        },
        RequestType::TruncateRequest => {
            let truncate_request = request.request_as_truncate_request().unwrap();
            let file = DistributedFileResponder::new(truncate_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.truncate(truncate_request.new_length(), truncate_request.forward());
        },
        RequestType::UnlinkRequest => {
            let unlink_request = request.request_as_unlink_request().unwrap();
            let file = DistributedFileResponder::new(unlink_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.unlink(unlink_request.forward());
        },
        RequestType::WriteRequest => {
            let write_request = request.request_as_write_request().unwrap();
            let file = DistributedFileResponder::new(write_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.write(write_request.offset(), write_request.data(), write_request.forward());
        },
        RequestType::UtimensRequest => {
            let utimens_request = request.request_as_utimens_request().unwrap();
            let file = DistributedFileResponder::new(utimens_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.utimens(utimens_request.atime().map(|x| x.seconds()).unwrap_or(0),
                        utimens_request.atime().map(|x| x.nanos()).unwrap_or(0),
                         utimens_request.mtime().map(|x| x.seconds()).unwrap_or(0),
                         utimens_request.mtime().map(|x| x.nanos()).unwrap_or(0),
                         utimens_request.forward());
        },
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let file = DistributedFileResponder::new(readdir_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.readdir();
        },
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let file = DistributedFileResponder::new(getattr_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.getattr();
        },
        RequestType::MkdirRequest => {
            let mkdir_request = request.request_as_mkdir_request().unwrap();
            let file = DistributedFileResponder::new(mkdir_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
            response = file.mkdir(mkdir_request.mode(), mkdir_request.forward()).map(|_| {
                let file = DistributedFileResponder::new(mkdir_request.path().to_string(), context.data_dir.clone(), &context.peers, builder);
                // TODO: probably possible to hit a distributed race here
                file.getattr().expect("getattr failed on newly created file")
            });
        },
        RequestType::NONE => unreachable!()
    }

    match response {
        Ok((response_type, response_offset)) => {
            let mut generic_response_builder = GenericResponseBuilder::new(builder);
            generic_response_builder.add_response_type(response_type);
            generic_response_builder.add_response(response_offset);

            let final_response_offset = generic_response_builder.finish();
            builder.finish_size_prefixed(final_response_offset, None);
        },
        Err(e) => {
            // Reset builder in case a partial response was written
            builder.reset();
            let error_code;
            if e.kind() == std::io::ErrorKind::NotFound {
                error_code = ErrorCode::DoesNotExist;
            }
            else if e.kind() == std::io::ErrorKind::Other {
                error_code = known_error_code.unwrap();
            }
            else {
                error_code = ErrorCode::Uncategorized;
            }
            let args = ErrorResponseArgs {error_code};
            let response_offset = ErrorResponse::create(builder, &args).as_union_value();
            let mut generic_response_builder = GenericResponseBuilder::new(builder);
            generic_response_builder.add_response_type(ResponseType::ErrorResponse);
            generic_response_builder.add_response(response_offset);

            let final_response_offset = generic_response_builder.finish();
            builder.finish_size_prefixed(final_response_offset, None);
        }
    }
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
                let builder = FlatBufferBuilder::new();
                let conn = reader.fold((writer, builder), move |(writer, mut builder), frame| {
                    let request = get_root_as_generic_request(&frame);
                    builder.reset();
                    handler(request, &cloned, &mut builder);
                    let writable = WritableFlatBuffer {buffer: builder};
                    tokio::io::write_all(writer, writable).map(|(writer, written)| (writer, written.buffer))
                });

                tokio::spawn(conn.map(|_| ()).map_err(|_| ()))
            });

        tokio::runtime::run(server.map(|_| ()));
    }
}
