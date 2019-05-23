use std::error::Error;
use std::fs;

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures::Stream;
use tokio::codec::length_delimited;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::file_handler::file_request_handler;
use crate::generated::*;
use crate::raft_manager::RaftManager;
use crate::utils::{empty_response, is_raft_request, is_write_request, WritableFlatBuffer};
use futures::future::{ok, result};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::timer::Interval;

pub fn raft_message_handler<'a, 'b>(
    request: GenericRequest<'a>,
    raft_manager: &RaftManager,
    mut builder: FlatBufferBuilder<'b>,
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
            response = Box::new(result(empty_response(builder)));
        }
        RequestType::LatestCommitRequest => {
            let index = raft_manager.get_latest_local_commit();
            let mut response_builder = LatestCommitResponseBuilder::new(&mut builder);
            response_builder.add_index(index);
            let response_offset = response_builder.finish().as_union_value();
            response = Box::new(ok((
                builder,
                ResponseType::LatestCommitResponse,
                response_offset,
            )));
        }
        RequestType::GetLeaderRequest => {
            let leader_future = raft_manager
                .get_leader()
                .map(move |leader_id| {
                    let mut response_builder = NodeIdResponseBuilder::new(&mut builder);
                    response_builder.add_node_id(leader_id);
                    let response_offset = response_builder.finish().as_union_value();
                    (builder, ResponseType::NodeIdResponse, response_offset)
                })
                .map_err(|_| ErrorCode::Uncategorized);

            response = Box::new(leader_future);
        }
        RequestType::NONE => unreachable!(),
    }

    response.map(|(mut builder, response_type, response_offset)| {
        let mut generic_response_builder = GenericResponseBuilder::new(&mut builder);
        generic_response_builder.add_response_type(response_type);
        generic_response_builder.add_response(response_offset);

        let final_response_offset = generic_response_builder.finish();
        builder.finish_size_prefixed(final_response_offset, None);

        builder
    })
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
                    let builder_future: Box<
                        Future<Item = FlatBufferBuilder, Error = ErrorCode> + Send,
                    >;
                    if is_raft_request(request.request_type()) {
                        builder_future =
                            Box::new(raft_message_handler(request, &cloned_raft, builder));
                    } else if is_write_request(request.request_type()) {
                        builder_future = Box::new(
                            cloned_raft
                                .propose(request, builder)
                                .map_err(|_| ErrorCode::Uncategorized),
                        );
                    } else {
                        // Sync to ensure replicas serve latest data
                        let cloned_raft2 = cloned_raft.clone();
                        let cloned2 = cloned.clone();
                        let after_sync = cloned_raft
                            .get_latest_commit_from_leader()
                            .map(move |latest_commit| cloned_raft2.sync(latest_commit))
                            .flatten()
                            .map_err(|_| ErrorCode::Uncategorized);
                        let read_after_sync = after_sync
                            .map(move |_| {
                                let request = get_root_as_generic_request(&frame);
                                file_request_handler(request, &cloned2, builder)
                            })
                            .flatten();
                        builder_future = Box::new(read_after_sync);
                    }
                    let builder_future2: Box<
                        Future<Item = FlatBufferBuilder, Error = std::io::Error> + Send,
                    >;
                    builder_future2 = Box::new(builder_future.or_else(|error_code| {
                        let mut builder = FlatBufferBuilder::new();
                        let args = ErrorResponseArgs { error_code };
                        let response_offset =
                            ErrorResponse::create(&mut builder, &args).as_union_value();
                        let mut generic_response_builder =
                            GenericResponseBuilder::new(&mut builder);
                        generic_response_builder.add_response_type(ResponseType::ErrorResponse);
                        generic_response_builder.add_response(response_offset);

                        let final_response_offset = generic_response_builder.finish();
                        builder.finish_size_prefixed(final_response_offset, None);

                        Ok(builder)
                    }));
                    builder_future2
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
