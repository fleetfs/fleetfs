use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;

use crate::generated::*;
use crate::utils::{finalize_request, response_or_error};
use byteorder::{ByteOrder, LittleEndian};
use futures::future::ok;
use futures::Future;
use log::error;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;

// TODO: should have a larger pool for connections to the leader, and smaller for other peers
const POOL_SIZE: usize = 8;

pub struct PeerClient {
    server_ip_port: SocketAddr,
    pool: Arc<Mutex<Vec<TcpStream>>>,
}

impl PeerClient {
    pub fn new(server_ip_port: SocketAddr) -> PeerClient {
        PeerClient {
            server_ip_port,
            pool: Arc::new(Mutex::new(vec![])),
        }
    }

    fn connect(&self) -> impl Future<Item = TcpStream, Error = std::io::Error> + Send {
        let mut locked = self.pool.lock().unwrap();
        let result: Box<Future<Item = TcpStream, Error = std::io::Error> + Send>;
        if let Some(stream) = locked.pop() {
            result = Box::new(ok(stream));
        } else {
            // TODO: should have an upper limit on the number of outstanding connections
            result = Box::new(TcpStream::connect(&self.server_ip_port));
        }

        result
    }

    fn return_connection(pool: Arc<Mutex<Vec<TcpStream>>>, connection: TcpStream) {
        let mut locked = pool.lock().unwrap();
        if locked.len() < POOL_SIZE {
            locked.push(connection);
        }
    }

    pub fn send_and_receive_length_prefixed(
        &self,
        data: Vec<u8>,
    ) -> impl Future<Item = Vec<u8>, Error = std::io::Error> {
        let pool = self.pool.clone();

        self.connect()
            .map(move |tcp_stream| {
                tokio::io::write_all(tcp_stream, data)
                    .map(|(stream, _)| {
                        let response_size = vec![0; 4];
                        tokio::io::read_exact(stream, response_size)
                            .map(|(stream, response_size_buffer)| {
                                let size = LittleEndian::read_u32(&response_size_buffer);
                                let buffer = vec![0; size as usize];
                                tokio::io::read_exact(stream, buffer).map(|(stream, data)| {
                                    PeerClient::return_connection(pool, stream);
                                    data
                                })
                            })
                            .flatten()
                    })
                    .flatten()
            })
            .flatten()
    }

    pub fn send_raft_message(&self, message: Message) -> impl Future<Item = (), Error = ()> {
        let serialized_message = message.write_to_bytes().unwrap();
        let mut builder = FlatBufferBuilder::new();
        let data_offset = builder.create_vector_direct(&serialized_message);
        let mut request_builder = RaftRequestBuilder::new(&mut builder);
        request_builder.add_message(data_offset);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RaftRequest, finish_offset);

        self.send_and_receive_length_prefixed(builder.finished_data().to_vec())
            .map(|_| ())
            .map_err(|e| error!("Error sending Raft message: {:?}", e))
    }

    pub fn get_latest_commit(&self) -> impl Future<Item = u64, Error = ()> {
        let mut builder = FlatBufferBuilder::new();
        let request_builder = LatestCommitRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::LatestCommitRequest,
            finish_offset,
        );

        self.send_and_receive_length_prefixed(builder.finished_data().to_vec())
            .map(|response| {
                response_or_error(&response)
                    .unwrap()
                    .response_as_latest_commit_response()
                    .unwrap()
                    .index()
            })
            .map_err(|e| error!("Error reading latest commit: {:?}", e))
    }

    pub fn filesystem_checksum(&self) -> impl Future<Item = Vec<u8>, Error = std::io::Error> {
        let mut builder = FlatBufferBuilder::new();
        let request_builder = FilesystemChecksumRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::FilesystemChecksumRequest,
            finish_offset,
        );

        self.send_and_receive_length_prefixed(builder.finished_data().to_vec())
            .map(|response| {
                response_or_error(&response)
                    .unwrap()
                    .response_as_read_response()
                    .unwrap()
                    .data()
                    .to_vec()
            })
    }
}
