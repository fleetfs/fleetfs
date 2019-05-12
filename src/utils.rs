use flatbuffers::{WIPOffset, UnionWIPOffset, FlatBufferBuilder};

use crate::generated::{ResponseType, EmptyResponseBuilder};

pub type ResultResponse = Result<(ResponseType, WIPOffset<UnionWIPOffset>), std::io::Error>;

pub fn empty_response(buffer: &mut FlatBufferBuilder) -> ResultResponse {
    let response_builder = EmptyResponseBuilder::new(buffer);
    let offset = response_builder.finish().as_union_value();
    return Ok((ResponseType::EmptyResponse, offset));
}


