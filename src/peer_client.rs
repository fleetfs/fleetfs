use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;

use crate::generated::*;
use crate::utils::{finalize_request, response_or_error};
use futures::stream::Stream;
use futures::Future;
use log::error;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use tokio::codec::length_delimited;
use tokio::net::TcpStream;

pub struct PeerClient {
    server_ip_port: SocketAddr,
}

impl PeerClient {
    pub fn new(server_ip_port: SocketAddr) -> PeerClient {
        PeerClient { server_ip_port }
    }

    pub fn send_and_receive_length_prefixed(
        &self,
        data: Vec<u8>,
    ) -> impl Future<Item = Vec<u8>, Error = std::io::Error> {
        // TODO: find a way to use a connection pool
        let tcp_stream = TcpStream::connect(&self.server_ip_port);

        tcp_stream
            .map(move |tcp_stream| {
                tokio::io::write_all(tcp_stream, data)
                    .map(|(stream, _)| {
                        let reader = length_delimited::Builder::new()
                            .little_endian()
                            .new_read(stream);

                        reader
                            .take(1)
                            .map(|response| response.to_vec())
                            .collect()
                            .map(|mut x| x.pop().unwrap())
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
