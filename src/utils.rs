use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::generated::*;

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

pub fn finalize_request(
    builder: &mut FlatBufferBuilder,
    request_type: RequestType,
    finish_offset: WIPOffset<UnionWIPOffset>,
) {
    let mut generic_request_builder = GenericRequestBuilder::new(builder);
    generic_request_builder.add_request_type(request_type);
    generic_request_builder.add_request(finish_offset);
    let finish_offset = generic_request_builder.finish();
    builder.finish_size_prefixed(finish_offset, None);
}

pub fn response_or_error(buffer: &[u8]) -> Result<GenericResponse, ErrorCode> {
    let response = flatbuffers::get_root::<GenericResponse>(buffer);
    if response.response_type() == ResponseType::ErrorResponse {
        let error = response.response_as_error_response().unwrap();
        return Err(error.error_code());
    }
    return Ok(response);
}
