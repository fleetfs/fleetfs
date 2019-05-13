use std::error::Error;
use std::fs;

use flatbuffers::FlatBufferBuilder;
use futures::Stream;
use tokio::codec::length_delimited;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::client::NodeClient;
use crate::distributed_file::DistributedFileResponder;
use crate::generated::*;
use crate::local_storage::LocalStorage;
use crate::utils::{empty_response, ResultResponse};
use std::net::SocketAddr;
use std::path::Path;

fn fsck(context: &LocalContext) -> Result<(), ErrorCode> {
    let local_storage = LocalStorage::new(&context.data_dir);
    let checksum = local_storage
        .checksum()
        .map_err(|_| ErrorCode::Uncategorized)?;
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

fn handler<'a>(
    request: GenericRequest<'a>,
    context: &LocalContext,
    builder: &mut FlatBufferBuilder,
) {
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
        }
        RequestType::FilesystemChecksumRequest => {
            response = checksum_request(&context.data_dir, builder);
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            let file = DistributedFileResponder::new(
                read_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.read(read_request.offset(), read_request.read_size());
        }
        RequestType::HardlinkRequest => {
            let hardlink_request = request.request_as_hardlink_request().unwrap();
            let file = DistributedFileResponder::new(
                hardlink_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.hardlink(&hardlink_request.new_path(), hardlink_request.forward());
        }
        RequestType::RenameRequest => {
            let rename_request = request.request_as_rename_request().unwrap();
            let file = DistributedFileResponder::new(
                rename_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.rename(&rename_request.new_path(), rename_request.forward());
        }
        RequestType::ChmodRequest => {
            let chmod_request = request.request_as_chmod_request().unwrap();
            let file = DistributedFileResponder::new(
                chmod_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.chmod(chmod_request.mode(), chmod_request.forward());
        }
        RequestType::TruncateRequest => {
            let truncate_request = request.request_as_truncate_request().unwrap();
            let file = DistributedFileResponder::new(
                truncate_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.truncate(truncate_request.new_length(), truncate_request.forward());
        }
        RequestType::UnlinkRequest => {
            let unlink_request = request.request_as_unlink_request().unwrap();
            let file = DistributedFileResponder::new(
                unlink_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.unlink(unlink_request.forward());
        }
        RequestType::WriteRequest => {
            let write_request = request.request_as_write_request().unwrap();
            let file = DistributedFileResponder::new(
                write_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.write(
                write_request.offset(),
                write_request.data(),
                write_request.forward(),
            );
        }
        RequestType::UtimensRequest => {
            let utimens_request = request.request_as_utimens_request().unwrap();
            let file = DistributedFileResponder::new(
                utimens_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.utimens(
                utimens_request.atime().map(Timestamp::seconds).unwrap_or(0),
                utimens_request.atime().map(Timestamp::nanos).unwrap_or(0),
                utimens_request.mtime().map(Timestamp::seconds).unwrap_or(0),
                utimens_request.mtime().map(Timestamp::nanos).unwrap_or(0),
                utimens_request.forward(),
            );
        }
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let file = DistributedFileResponder::new(
                readdir_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.readdir();
        }
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let file = DistributedFileResponder::new(
                getattr_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file.getattr();
        }
        RequestType::MkdirRequest => {
            let mkdir_request = request.request_as_mkdir_request().unwrap();
            let file = DistributedFileResponder::new(
                mkdir_request.path().to_string(),
                context.data_dir.clone(),
                &context.peers,
                builder,
            );
            response = file
                .mkdir(mkdir_request.mode(), mkdir_request.forward())
                .map(|_| {
                    let file = DistributedFileResponder::new(
                        mkdir_request.path().to_string(),
                        context.data_dir.clone(),
                        &context.peers,
                        builder,
                    );
                    // TODO: probably possible to hit a distributed race here
                    file.getattr()
                        .expect("getattr failed on newly created file")
                });
        }
        RequestType::NONE => unreachable!(),
    }

    match response {
        Ok((response_type, response_offset)) => {
            let mut generic_response_builder = GenericResponseBuilder::new(builder);
            generic_response_builder.add_response_type(response_type);
            generic_response_builder.add_response(response_offset);

            let final_response_offset = generic_response_builder.finish();
            builder.finish_size_prefixed(final_response_offset, None);
        }
        Err(e) => {
            // Reset builder in case a partial response was written
            builder.reset();
            let error_code;
            if e.kind() == std::io::ErrorKind::NotFound {
                error_code = ErrorCode::DoesNotExist;
            } else if e.kind() == std::io::ErrorKind::Other {
                error_code = known_error_code.unwrap();
            } else {
                error_code = ErrorCode::Uncategorized;
            }
            let args = ErrorResponseArgs { error_code };
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
    peers: Vec<SocketAddr>,
}

impl LocalContext {
    pub fn new(data_dir: &str, peers: Vec<SocketAddr>) -> LocalContext {
        LocalContext {
            data_dir: data_dir.to_string(),
            peers,
        }
    }
}

struct WritableFlatBuffer<'a> {
    buffer: FlatBufferBuilder<'a>,
}

impl<'a> AsRef<[u8]> for WritableFlatBuffer<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.finished_data()
    }
}

pub struct Node {
    context: LocalContext,
    port: u16,
}

impl Node {
    pub fn new(data_dir: String, port: u16, peers: Vec<SocketAddr>) -> Node {
        Node {
            context: LocalContext::new(Path::new(&data_dir).join("data").to_str().unwrap(), peers),
            port,
        }
    }

    pub fn run(self) {
        if let Err(why) = fs::create_dir_all(&self.context.data_dir) {
            panic!("Couldn't create storage dir: {}", why.description());
        };

        let address = ([127, 0, 0, 1], self.port).into();
        let listener = TcpListener::bind(&address).expect("unable to bind API listener");

        let context = self.context.clone();
        let server = listener
            .incoming()
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
                    let writable = WritableFlatBuffer { buffer: builder };
                    tokio::io::write_all(writer, writable)
                        .map(|(writer, written)| (writer, written.buffer))
                });

                tokio::spawn(conn.map(|_| ()).map_err(|_| ()))
            });

        tokio::runtime::run(server.map(|_| ()));
    }
}
