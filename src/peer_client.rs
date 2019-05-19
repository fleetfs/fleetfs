use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;

use crate::generated::*;
use crate::tcp_client::TcpClient;
use crate::utils::{finalize_request, response_or_error};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;

pub struct PeerClient {
    tcp_client: TcpClient,
}

impl PeerClient {
    pub fn new(server_ip_port: SocketAddr) -> PeerClient {
        PeerClient {
            tcp_client: TcpClient::new(server_ip_port),
        }
    }

    fn send<'b>(
        &self,
        request: &[u8],
        buffer: &'b mut Vec<u8>,
    ) -> Result<GenericResponse<'b>, ErrorCode> {
        self.tcp_client
            .send_and_receive_length_prefixed(request, buffer.as_mut())
            .map_err(|_| ErrorCode::Uncategorized)?;
        return response_or_error(buffer);
    }

    pub fn send_raft_message(&self, message: Message) -> Result<(), ErrorCode> {
        let serialized_message = message.write_to_bytes().unwrap();
        let mut builder = FlatBufferBuilder::new();
        let data_offset = builder.create_vector_direct(&serialized_message);
        let mut request_builder = RaftRequestBuilder::new(&mut builder);
        request_builder.add_message(data_offset);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RaftRequest, finish_offset);

        let mut buffer = vec![];
        let response = self.send(builder.finished_data(), &mut buffer)?;
        // TODO: maybe receive message back to reduce roundtrips?
        response.response_as_empty_response().unwrap();

        return Ok(());
    }
}
