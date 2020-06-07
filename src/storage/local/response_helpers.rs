use crate::base::{FlatBufferWithResponse, LengthPrefixedVec, ResultResponse};
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
    let refs: Vec<&str> = xattrs.iter().map(AsRef::as_ref).collect();
    let offset = builder.create_vector_of_strings(&refs);
    let mut response_builder = XattrsResponseBuilder::new(&mut builder);
    response_builder.add_xattrs(offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::XattrsResponse, response_offset));
}

pub fn to_fast_read_response(
    builder: FlatBufferBuilder,
    response: Result<LengthPrefixedVec, ErrorCode>,
) -> FlatBufferWithResponse {
    match response {
        Ok(mut data) => {
            data.push(ErrorCode::DefaultValueNotAnError as u8);
            FlatBufferWithResponse::with_separate_response(builder, data)
        }
        Err(error_code) => {
            let mut data = LengthPrefixedVec::zeros(0);
            data.push(error_code as u8);
            FlatBufferWithResponse::with_separate_response(builder, data)
        }
    }
}

pub fn to_read_response<'a>(mut builder: FlatBufferBuilder<'a>, data: &[u8]) -> ResultResponse<'a> {
    let data_offset = builder.create_vector_direct(data);
    let mut response_builder = ReadResponseBuilder::new(&mut builder);
    response_builder.add_data(data_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::ReadResponse, response_offset));
}

pub fn to_inode_response(mut builder: FlatBufferBuilder, inode: u64) -> ResultResponse {
    let mut response_builder = InodeResponseBuilder::new(&mut builder);
    response_builder.add_inode(inode);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::InodeResponse, response_offset));
}

pub fn to_write_response(mut builder: FlatBufferBuilder, length: u32) -> ResultResponse {
    let mut response_builder = WrittenResponseBuilder::new(&mut builder);
    response_builder.add_bytes_written(length);
    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::WrittenResponse, offset));
}
