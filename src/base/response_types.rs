use crate::generated::ResponseType;
use crate::ErrorCode;
use byteorder::{ByteOrder, LittleEndian};
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
    response: Option<Vec<u8>>,
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
        response: Vec<u8>,
    ) -> FlatBufferWithResponse<'a> {
        // TODO: optimize out this copy
        let mut prefixed_response = vec![0; response.len() + 4];
        LittleEndian::write_u32(&mut prefixed_response[..4], response.len() as u32);
        prefixed_response[4..].copy_from_slice(&response);
        FlatBufferWithResponse {
            buffer,
            response: Some(prefixed_response),
        }
    }

    pub fn into_buffer(self) -> FlatBufferBuilder<'a> {
        self.buffer
    }
}

impl<'a> AsRef<[u8]> for FlatBufferWithResponse<'a> {
    fn as_ref(&self) -> &[u8] {
        if let Some(ref response) = self.response {
            response
        } else {
            self.buffer.finished_data()
        }
    }
}
