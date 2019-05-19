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
use crate::raft_manager::RaftManager;
use crate::utils::{
    empty_response, is_raft_request, is_write_request, ResultResponse, WritableFlatBuffer,
};
use futures::future::ok;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::timer::Interval;

fn fsck(local_storage: &LocalStorage, context: &LocalContext) -> Result<(), ErrorCode> {
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

fn checksum_request(
    local_storage: &LocalStorage,
    builder: &mut FlatBufferBuilder,
) -> ResultResponse {
    let checksum = local_storage.checksum()?;
    let data_offset = builder.create_vector_direct(&checksum);
    let mut response_builder = ReadResponseBuilder::new(builder);
    response_builder.add_data(data_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((ResponseType::ReadResponse, response_offset));
}
pub fn raft_message_handler<'a>(
    request: GenericRequest<'a>,
    raft_manager: &RaftManager,
    builder: &mut FlatBufferBuilder,
) {
    let response;

    match request.request_type() {
        RequestType::FilesystemCheckRequest => unreachable!(),
        RequestType::FilesystemChecksumRequest => unreachable!(),
        RequestType::ReadRequest => unreachable!(),
        RequestType::HardlinkRequest => unreachable!(),
        RequestType::RenameRequest => unreachable!(),
        RequestType::ChmodRequest => unreachable!(),
        RequestType::TruncateRequest => unreachable!(),
        RequestType::UnlinkRequest => unreachable!(),
        RequestType::WriteRequest => unreachable!(),
        RequestType::UtimensRequest => unreachable!(),
        RequestType::ReaddirRequest => unreachable!(),
        RequestType::GetattrRequest => unreachable!(),
        RequestType::MkdirRequest => unreachable!(),
        RequestType::RaftRequest => {
            let raft_request = request.request_as_raft_request().unwrap();
            let mut deserialized_message = Message::new();
            deserialized_message
                .merge_from_bytes(raft_request.message())
                .unwrap();
            raft_manager
                .apply_messages(&[deserialized_message])
                .unwrap();
            response = empty_response(builder);
        }
        RequestType::LatestCommitRequest => {
            let index = raft_manager.get_latest_local_commit();
            let mut response_builder = LatestCommitResponseBuilder::new(builder);
            response_builder.add_index(index);
            let response_offset = response_builder.finish().as_union_value();
            response = Ok((ResponseType::LatestCommitResponse, response_offset));
        }
        RequestType::NONE => unreachable!(),
    }

    let (response_type, response_offset) = response.unwrap();
    let mut generic_response_builder = GenericResponseBuilder::new(builder);
    generic_response_builder.add_response_type(response_type);
    generic_response_builder.add_response(response_offset);

    let final_response_offset = generic_response_builder.finish();
    builder.finish_size_prefixed(final_response_offset, None);
}

pub fn handler<'a>(
    request: GenericRequest<'a>,
    local_storage: &LocalStorage,
    context: &LocalContext,
    builder: &mut FlatBufferBuilder,
) {
    let response;
    let mut known_error_code = None;

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            response = match fsck(local_storage, context) {
                Ok(_) => empty_response(builder),
                Err(error_code) => {
                    known_error_code = Some(error_code);
                    Err(std::io::Error::from(std::io::ErrorKind::Other))
                }
            };
        }
        RequestType::FilesystemChecksumRequest => {
            response = checksum_request(local_storage, builder);
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            let file = DistributedFileResponder::new(
                read_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.read(read_request.offset(), read_request.read_size());
        }
        RequestType::HardlinkRequest => {
            let hardlink_request = request.request_as_hardlink_request().unwrap();
            let file = DistributedFileResponder::new(
                hardlink_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.hardlink(&hardlink_request.new_path());
        }
        RequestType::RenameRequest => {
            let rename_request = request.request_as_rename_request().unwrap();
            let file = DistributedFileResponder::new(
                rename_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.rename(&rename_request.new_path());
        }
        RequestType::ChmodRequest => {
            let chmod_request = request.request_as_chmod_request().unwrap();
            let file = DistributedFileResponder::new(
                chmod_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.chmod(chmod_request.mode());
        }
        RequestType::TruncateRequest => {
            let truncate_request = request.request_as_truncate_request().unwrap();
            let file = DistributedFileResponder::new(
                truncate_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.truncate(truncate_request.new_length());
        }
        RequestType::UnlinkRequest => {
            let unlink_request = request.request_as_unlink_request().unwrap();
            let file = DistributedFileResponder::new(
                unlink_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.unlink();
        }
        RequestType::WriteRequest => {
            let write_request = request.request_as_write_request().unwrap();
            let file = DistributedFileResponder::new(
                write_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.write(write_request.offset(), write_request.data());
        }
        RequestType::UtimensRequest => {
            let utimens_request = request.request_as_utimens_request().unwrap();
            let file = DistributedFileResponder::new(
                utimens_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.utimens(
                utimens_request.atime().map(Timestamp::seconds).unwrap_or(0),
                utimens_request.atime().map(Timestamp::nanos).unwrap_or(0),
                utimens_request.mtime().map(Timestamp::seconds).unwrap_or(0),
                utimens_request.mtime().map(Timestamp::nanos).unwrap_or(0),
            );
        }
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let file = DistributedFileResponder::new(
                readdir_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.readdir();
        }
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let file = DistributedFileResponder::new(
                getattr_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.getattr();
        }
        RequestType::MkdirRequest => {
            let mkdir_request = request.request_as_mkdir_request().unwrap();
            let file = DistributedFileResponder::new(
                mkdir_request.path().to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = file.mkdir(mkdir_request.mode()).map(|_| {
                let file = DistributedFileResponder::new(
                    mkdir_request.path().to_string(),
                    context.data_dir.clone(),
                    builder,
                );
                // TODO: probably possible to hit a distributed race here
                file.getattr()
                    .expect("getattr failed on newly created file")
            });
        }
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
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
pub struct LocalContext {
    pub data_dir: String,
    pub peers: Vec<SocketAddr>,
    pub node_id: u64,
}

impl LocalContext {
    pub fn new(data_dir: &str, peers: Vec<SocketAddr>, node_id: u64) -> LocalContext {
        LocalContext {
            data_dir: data_dir.to_string(),
            peers,
            node_id,
        }
    }
}

pub struct Node {
    context: LocalContext,
    raft_manager: RaftManager<'static>,
    port: u16,
}

impl Node {
    pub fn new(node_dir: &str, port: u16, peers: Vec<SocketAddr>) -> Node {
        let data_dir = Path::new(node_dir).join("data");
        // TODO huge hack. Should be generated randomly and then dynamically discovered
        // Unique ID of node within the cluster. Never 0.
        let node_id = u64::from(port);
        let context = LocalContext::new(data_dir.to_str().unwrap(), peers, node_id);
        Node {
            context: context.clone(),
            raft_manager: RaftManager::new(context.clone()),
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
        let raft_manager = Arc::new(self.raft_manager);
        let raft_manager_cloned = raft_manager.clone();
        let server = listener
            .incoming()
            .map_err(|e| eprintln!("accept connection failed = {:?}", e))
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let reader = length_delimited::Builder::new()
                    .little_endian()
                    .new_read(reader);

                let cloned = context.clone();
                let cloned_raft = raft_manager.clone();
                let builder = FlatBufferBuilder::new();
                let conn = reader.fold((writer, builder), move |(writer, mut builder), frame| {
                    let request = get_root_as_generic_request(&frame);
                    builder.reset();
                    // TODO: not sure we really need this Box
                    let builder_future: Box<
                        Future<Item = FlatBufferBuilder, Error = std::io::Error> + Send,
                    >;
                    if is_raft_request(request.request_type()) {
                        raft_message_handler(request, &cloned_raft, &mut builder);
                        builder_future = Box::new(ok(builder));
                    } else if is_write_request(request.request_type()) {
                        builder_future = Box::new(
                            cloned_raft
                                .propose(request, builder)
                                .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other)),
                        );
                    } else {
                        // Sync to ensure replicas serve latest data
                        let cloned_raft2 = cloned_raft.clone();
                        let cloned2 = cloned.clone();
                        let after_sync = cloned_raft
                            .get_latest_commit_from_leader()
                            .map(move |latest_commit| cloned_raft2.sync(latest_commit))
                            .flatten();
                        let read_after_sync = after_sync
                            .map(move |_| {
                                let request = get_root_as_generic_request(&frame);
                                let local_storage = LocalStorage::new(cloned2.clone());
                                handler(request, &local_storage, &cloned2, &mut builder);
                                builder
                            })
                            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other));
                        builder_future = Box::new(read_after_sync);
                    }
                    builder_future
                        .map(|builder| {
                            let writable = WritableFlatBuffer::new(builder);
                            tokio::io::write_all(writer, writable)
                        })
                        .flatten()
                        .map(|(writer, written)| (writer, written.into_buffer()))
                });

                tokio::spawn(conn.map(|_| ()).map_err(|_| ()))
            });

        let background_raft = Interval::new(Instant::now(), Duration::from_millis(100))
            .for_each(move |_| {
                raft_manager_cloned.background_tick();
                Ok(())
            })
            .map_err(|e| panic!("Background Raft thread failed error: {:?}", e));

        // TODO: currently we run single threaded to uncover deadlocks more easily
        let mut runtime = tokio::runtime::Builder::new()
            .core_threads(1)
            .build()
            .unwrap();
        runtime.spawn(server);
        runtime.block_on_all(background_raft.map(|_| ())).unwrap();
    }
}
