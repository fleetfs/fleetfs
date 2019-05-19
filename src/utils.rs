use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::generated::{EmptyResponseBuilder, RequestType, ResponseType};

pub type ResultResponse = Result<(ResponseType, WIPOffset<UnionWIPOffset>), std::io::Error>;

pub fn empty_response(buffer: &mut FlatBufferBuilder) -> ResultResponse {
    let response_builder = EmptyResponseBuilder::new(buffer);
    let offset = response_builder.finish().as_union_value();
    return Ok((ResponseType::EmptyResponse, offset));
}

pub fn is_write_request(request_type: RequestType) -> bool {
    match request_type {
        RequestType::ReadRequest => false,
        RequestType::ReaddirRequest => false,
        RequestType::GetattrRequest => false,
        RequestType::FilesystemCheckRequest => false,
        RequestType::FilesystemChecksumRequest => false,
        RequestType::UtimensRequest => true,
        RequestType::ChmodRequest => true,
        RequestType::HardlinkRequest => true,
        RequestType::TruncateRequest => true,
        RequestType::UnlinkRequest => true,
        RequestType::RenameRequest => true,
        RequestType::MkdirRequest => true,
        RequestType::WriteRequest => true,
        RequestType::RaftRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }
}
