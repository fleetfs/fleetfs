use crate::generated::*;
use crate::storage::raft_manager::RaftManager;
use crate::utils::{empty_response, finalize_response};
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures::future::{ok, result};
use futures::Future;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;

pub fn raft_handler<'a, 'b>(
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
        RequestType::ReadRawRequest => unreachable!(),
        RequestType::LookupRequest => unreachable!(),
        RequestType::GetXattrRequest => unreachable!(),
        RequestType::ListXattrsRequest => unreachable!(),
        RequestType::SetXattrRequest => unreachable!(),
        RequestType::RemoveXattrRequest => unreachable!(),
        RequestType::HardlinkRequest => unreachable!(),
        RequestType::RenameRequest => unreachable!(),
        RequestType::ChmodRequest => unreachable!(),
        RequestType::ChownRequest => unreachable!(),
        RequestType::TruncateRequest => unreachable!(),
        RequestType::FsyncRequest => unreachable!(),
        RequestType::UnlinkRequest => unreachable!(),
        RequestType::RmdirRequest => unreachable!(),
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
        finalize_response(&mut builder, response_type, response_offset);
        builder
    })
}
