use crate::base::LengthPrefixedVec;
use crate::generated::{ErrorCode, ResponseType};
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

pub type ResultResponse<'a> = Result<FlatBufferResponse<'a>, ErrorCode>;

pub type FlatBufferResponse<'a> = (
    FlatBufferBuilder<'a>,
    ResponseType,
    WIPOffset<UnionWIPOffset>,
);

// A response to be sent back to the client, which by default is assumed to be in the FlatBufferBuilder
// but may be overriden with a different response. In that case the FlatBufferBuilder is just carried
// along, so that it can be reused.
pub struct FlatBufferWithResponse<'a> {
    buffer: FlatBufferBuilder<'a>,
    response: Option<LengthPrefixedVec>,
}

impl<'a> FlatBufferWithResponse<'a> {
    pub fn new(buffer: FlatBufferBuilder<'a>) -> FlatBufferWithResponse<'a> {
        FlatBufferWithResponse {
            buffer,
            response: None,
        }
    }

    pub fn with_separate_response(
        buffer: FlatBufferBuilder<'a>,
        response: LengthPrefixedVec,
    ) -> FlatBufferWithResponse<'a> {
        FlatBufferWithResponse {
            buffer,
            response: Some(response),
        }
    }

    pub fn into_buffer(self) -> FlatBufferBuilder<'a> {
        self.buffer
    }
}

impl<'a> AsRef<[u8]> for FlatBufferWithResponse<'a> {
    fn as_ref(&self) -> &[u8] {
        if let Some(ref response) = self.response {
            response.length_prefixed_bytes()
        } else {
            self.buffer.finished_data()
        }
    }
}
