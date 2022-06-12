use crate::base::message_types::{ErrorCode, RkyvGenericResponse};
use crate::base::ResultResponse;
use crate::generated::*;
use flatbuffers::FlatBufferBuilder;
use std::io::ErrorKind;

pub fn into_error_code(error: std::io::Error) -> ErrorCode {
    match error.kind() {
        ErrorKind::NotFound => ErrorCode::DoesNotExist,
        ErrorKind::Other => {
            if let Some(code) = error.raw_os_error() {
                if code == libc::EFBIG {
                    return ErrorCode::FileTooLarge;
                }
            }
            return ErrorCode::Uncategorized;
        }
        _ => ErrorCode::Uncategorized,
    }
}

pub fn to_xattrs_response<'a, T: AsRef<str>>(
    mut builder: FlatBufferBuilder<'a>,
    xattrs: &[T],
) -> ResultResponse<'a> {
    let rkyv_response = RkyvGenericResponse::Xattrs {
        attrs: xattrs.iter().map(|x| x.as_ref().to_string()).collect(),
    };
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::RkyvResponse, offset));
}

pub fn to_read_response<'a>(mut builder: FlatBufferBuilder<'a>, data: &[u8]) -> ResultResponse<'a> {
    let rkyv_response = RkyvGenericResponse::Read {
        data: data.to_vec(),
    };
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::RkyvResponse, response_offset));
}

pub fn to_inode_response(mut builder: FlatBufferBuilder, inode: u64) -> ResultResponse {
    let rkyv_response = RkyvGenericResponse::Inode { id: inode };
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::RkyvResponse, response_offset));
}

pub fn to_write_response(mut builder: FlatBufferBuilder, length: u32) -> ResultResponse {
    let rkyv_response = RkyvGenericResponse::Written {
        bytes_written: length,
    };
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::RkyvResponse, offset));
}
