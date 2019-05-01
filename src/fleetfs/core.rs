use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

use bytes::{Buf};
use flatbuffers::{FlatBufferBuilder, WIPOffset, UnionWIPOffset};
use futures::future;
use futures::Stream;
use hyper::{Body, Response, Server, StatusCode};
use hyper::Method;
use hyper::Request;
use hyper::rt::Future;
use hyper::service::service_fn;
use log::info;
use tokio::codec::length_delimited;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::fleetfs::client::PeerClient;
use crate::fleetfs::generated::*;
use std::os::linux::fs::MetadataExt;
use std::net::SocketAddr;
use filetime::FileTime;

pub const PATH_HEADER: &str = "X-FleetFS-Path";
pub const NO_FORWARD_HEADER: &str = "X-FleetFS-No-Forward";


pub type BoxFuture = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

struct DistributedFile {
    filename: String,
    local_data_dir: String,
    peers: Vec<PeerClient>
}

impl DistributedFile {
    fn new(filename: String, local_data_dir: String, peers: &[String], peers_v2: &Vec<SocketAddr>) -> DistributedFile {
        DistributedFile {
            // XXX: hack
            filename: filename.trim_start_matches('/').to_string(),
            local_data_dir,
            peers: peers_v2.iter().zip(peers.into_iter()).map(|(peer_v2, peer)| PeerClient::new(peer, *peer_v2)).collect()
        }
    }

    fn to_local_path(&self, path: &String) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    fn truncate(self, req: Request<Body>) -> BoxFuture {
        let new_length: u64 = req.uri().path()["/truncate/".len()..].parse().unwrap();
        let forward: bool = req.headers().get(NO_FORWARD_HEADER).is_none();
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                let path = Path::new(&self.local_data_dir).join(&self.filename);
                let display = path.display();

                let file = match File::create(&path) {
                    Err(why) => panic!("couldn't create {}: {}",
                                       display,
                                       why.description()),
                    Ok(file) => file,
                };
                file.set_len(new_length).unwrap();

                if forward {
                    for peer in self.peers {
                        peer.truncate(&self.filename, new_length);
                    }
                }

                Response::new(Body::from("done"))
            });

        Box::new(response)
    }

    fn write(self, req: Request<Body>) -> BoxFuture {
        let offset: u64 = req.uri().path()[1..].parse().unwrap();
        let forward: bool = req.headers().get(NO_FORWARD_HEADER).is_none();
        let response = req.into_body()
            .concat2()
            .map(move |chunk| {
                let bytes = chunk.bytes();
                let path = Path::new(&self.local_data_dir).join(&self.filename);
                let display = path.display();

                let mut file = match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&path) {
                    Err(why) => panic!("couldn't create {}: {}",
                                       display,
                                       why.description()),
                    Ok(file) => file,
                };

                match file.seek(SeekFrom::Start(offset)) {
                    Err(why) => {
                        panic!("couldn't seek to {} in {} because {}", offset, display,
                               why.description())
                    },
                    Ok(_) => {}
                }

                match file.write_all(bytes) {
                    Err(why) => {
                        panic!("couldn't write to {}: {}", display,
                               why.description())
                    },
                    Ok(_) => info!("successfully wrote to {}", display),
                }

                if forward {
                    for peer in self.peers {
                        peer.write(format!("/{}", &self.filename), offset, bytes);
                    }
                }

                Response::new(Body::from("done"))
            });

        Box::new(response)
    }

    fn mkdir(self, _mode: u16) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        let path = Path::new(&self.local_data_dir).join(&self.filename);
        fs::create_dir(path)?;
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

    fn getattr_v2(self, buffer: &mut FlatBufferBuilder) -> Result<WIPOffset<UnionWIPOffset>, std::io::Error> {
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
                peer.utimens(&self.filename, atime_secs, atime_nanos, mtime_secs, mtime_nanos);
            }
        }
        return filetime::set_file_times(local_path,
                                        FileTime::from_unix_time(atime_secs, atime_nanos as u32),
                                        FileTime::from_unix_time(mtime_secs, mtime_nanos as u32));
    }

    fn rename(self, new_path: &String, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        info!("Renaming file: {} to {}", self.filename, new_path);
        let local_path = self.to_local_path(&self.filename);
        let local_new_path = self.to_local_path(new_path);
        if forward {
            for peer in self.peers {
                // TODO make this async
                peer.rename(&self.filename, new_path);
            }
        }
        return fs::rename(local_path, local_new_path);
    }

    fn read_v2(self, offset: u64, size: u32) -> Result<Vec<u8>, std::io::Error> {
        assert_ne!(self.filename.len(), 0);

        info!("[v2 API] Reading file: {}. data_dir={}", self.filename, self.local_data_dir);
        let path = Path::new(&self.local_data_dir).join(&self.filename);
        let file = File::open(&path)?;

        let mut contents = vec![0; size as usize];
        let bytes_read = file.read_at(&mut contents, offset)?;
        contents.truncate(bytes_read);

        return Ok(contents);
    }

    fn unlink(self, req: Request<Body>) -> BoxFuture {
        let forward: bool = req.headers().get(NO_FORWARD_HEADER).is_none();
        assert_ne!(self.filename.len(), 0);

        info!("Deleting file");
        let path = Path::new(&self.local_data_dir).join(&self.filename);
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                fs::remove_file(path)
                    .expect("Something went wrong reading the file");

                Response::new(Body::from("success"))
            });

        let mut result: BoxFuture = Box::new(response);
        if forward {
            for peer in self.peers {
                let peer_request = peer.unlink(format!("{}", &self.filename));
                result = Box::new(result.join(peer_request).map(|(r, _)| r));
            }
        }

        return result;
    }

}

fn handler(req: Request<Body>, data_dir: String, peers: &[String], peers_v2: &Vec<SocketAddr>) -> BoxFuture {
    let mut response = Response::new(Body::empty());

    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap().trim_start_matches('/').to_string();

    let file = DistributedFile::new(filename, data_dir, peers, peers_v2);

    if req.method() == Method::POST && req.uri().path().starts_with("/truncate") {
        return file.truncate(req);
    }

    match (req.method(), req.uri().path()) {
        (&Method::DELETE, "/") => {
            return file.unlink(req);
        },
        (&Method::POST, _) => {
            return file.write(req);
        },
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        },
    };

    Box::new(future::ok(response))
}

fn handler_v2<'a, 'b>(request: GenericRequest<'a>, context: &LocalContext) -> FlatBufferBuilder<'b> {
    let mut builder = FlatBufferBuilder::new();
    let response_type;
    let response_offset;

    match request.request_type() {
        RequestType::ReadRequest => {
            response_type = ResponseType::ReadResponse;
            let read_request = request.request_as_read_request().unwrap();
            let file = DistributedFile::new(read_request.filename().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
            let data = file.read_v2(read_request.offset(), read_request.read_size());
            let data_offset = builder.create_vector_direct(&data.unwrap());
            let mut response_builder = ReadResponseBuilder::new(&mut builder);
            response_builder.add_data(data_offset);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::RenameRequest => {
            response_type = ResponseType::EmptyResponse;
            let rename_request = request.request_as_rename_request().unwrap();
            let file = DistributedFile::new(rename_request.path().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
            // TODO handle errors
            file.rename(&rename_request.new_path().to_string(), rename_request.forward()).unwrap();
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            response_offset = response_builder.finish().as_union_value();
        },
        RequestType::UtimensRequest => {
            response_type = ResponseType::EmptyResponse;
            let utimens_request = request.request_as_utimens_request().unwrap();
            let file = DistributedFile::new(utimens_request.path().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
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
            let file = DistributedFile::new(readdir_request.path().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
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
            let file = DistributedFile::new(getattr_request.filename().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
            match file.getattr_v2(&mut builder) {
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
            let file = DistributedFile::new(mkdir_request.filename().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
            match file.mkdir(mkdir_request.mode()) {
                Ok(_) => {
                    let file = DistributedFile::new(mkdir_request.filename().to_string(), context.data_dir.clone(), &context.peers, &context.peers_v2);
                    match file.getattr_v2(&mut builder) {
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
    peers: Vec<String>,
    peers_v2: Vec<SocketAddr>
}

impl LocalContext {
    pub fn new(data_dir: String, peers: Vec<String>, peers_v2: Vec<SocketAddr>) -> LocalContext {
        LocalContext {
            data_dir,
            peers,
            peers_v2
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
    port: u16,
    port_v2: u16
}

impl Node {
    pub fn new(data_dir: String, port: u16, port_v2: u16, peers: Vec<String>, peers_v2: Vec<SocketAddr>) -> Node {
        Node {
            context: LocalContext::new(data_dir, peers, peers_v2),
            port,
            port_v2
        }
    }

    pub fn run(self) {
        match fs::create_dir_all(&self.context.data_dir) {
            Err(why) => panic!("Couldn't create storage dir: {}", why.description()),
            Ok(_) => ()

        };

        let addr = ([127, 0, 0, 1], self.port).into();

        let context = self.context.clone();
        let new_service = move || {
            let cloned = context.clone();
            service_fn(move |req| handler(req, cloned.data_dir.clone(), &cloned.peers, &cloned.peers_v2))
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        // TCP based v2 API
        let addr_v2 = ([127, 0, 0, 1], self.port_v2).into();
        let listener = TcpListener::bind(&addr_v2)
            .expect("unable to bind API v2 listener");

        let context = self.context.clone();
        let server_v2 = listener.incoming()
            .map_err(|e| eprintln!("accept connection failed = {:?}", e))
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let reader = length_delimited::Builder::new()
                    .little_endian()
                    .new_read(reader);

                let cloned = context.clone();
                let conn = reader.fold(writer, move |writer, frame| {
                    let request = get_root_as_generic_request(&frame);
                    let response_builder = handler_v2(request, &cloned);
                    let writable = WritableFlatBuffer {buffer: response_builder};
                    tokio::io::write_all(writer, writable).map(|(writer, _)| writer)
                });

                tokio::spawn(conn.map(|_| ()).map_err(|_| ()))
            });

        hyper::rt::run(server.join(server_v2).map(|_| ()));
    }
}
