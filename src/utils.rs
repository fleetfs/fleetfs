use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::generated::*;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, ErrorKind};

pub type ResultResponse<'a> = Result<
    (
        FlatBufferBuilder<'a>,
        ResponseType,
        WIPOffset<UnionWIPOffset>,
    ),
    ErrorCode,
>;

pub fn empty_response(mut buffer: FlatBufferBuilder) -> ResultResponse {
    let response_builder = EmptyResponseBuilder::new(&mut buffer);
    let offset = response_builder.finish().as_union_value();
    return Ok((buffer, ResponseType::EmptyResponse, offset));
}

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

pub fn is_raft_request(request_type: RequestType) -> bool {
    match request_type {
        RequestType::ReadRequest => false,
        RequestType::ReadRawRequest => false,
        RequestType::ReaddirRequest => false,
        RequestType::AccessRequest => false,
        RequestType::GetattrRequest => false,
        RequestType::FilesystemCheckRequest => false,
        RequestType::FilesystemChecksumRequest => false,
        RequestType::GetXattrRequest => false,
        RequestType::ListXattrsRequest => false,
        RequestType::SetXattrRequest => false,
        RequestType::RemoveXattrRequest => false,
        RequestType::UtimensRequest => false,
        RequestType::ChmodRequest => false,
        RequestType::ChownRequest => false,
        RequestType::HardlinkRequest => false,
        RequestType::TruncateRequest => false,
        RequestType::FsyncRequest => false,
        RequestType::UnlinkRequest => false,
        RequestType::RmdirRequest => false,
        RequestType::RenameRequest => false,
        RequestType::MkdirRequest => false,
        RequestType::WriteRequest => false,
        RequestType::RaftRequest => true,
        RequestType::LatestCommitRequest => true,
        RequestType::GetLeaderRequest => true,
        RequestType::NONE => unreachable!(),
    }
}

// TODO: refactor flatbuffer message union to encode read/write/raft...etc
pub fn is_write_request(request_type: RequestType) -> bool {
    match request_type {
        RequestType::ReadRequest => false,
        RequestType::ReadRawRequest => false,
        RequestType::ReaddirRequest => false,
        RequestType::AccessRequest => false,
        RequestType::GetattrRequest => false,
        RequestType::FilesystemCheckRequest => false,
        RequestType::FilesystemChecksumRequest => false,
        RequestType::GetXattrRequest => false,
        RequestType::ListXattrsRequest => false,
        RequestType::SetXattrRequest => true,
        RequestType::RemoveXattrRequest => true,
        RequestType::UtimensRequest => true,
        RequestType::ChmodRequest => true,
        RequestType::ChownRequest => true,
        RequestType::HardlinkRequest => true,
        RequestType::TruncateRequest => true,
        RequestType::FsyncRequest => true,
        RequestType::UnlinkRequest => true,
        RequestType::RmdirRequest => true,
        RequestType::RenameRequest => true,
        RequestType::MkdirRequest => true,
        RequestType::WriteRequest => true,
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::GetLeaderRequest => unreachable!(),
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

pub fn finalize_response(
    builder: &mut FlatBufferBuilder,
    response_type: ResponseType,
    finish_offset: WIPOffset<UnionWIPOffset>,
) {
    let mut generic_response_builder = GenericResponseBuilder::new(builder);
    generic_response_builder.add_response_type(response_type);
    generic_response_builder.add_response(finish_offset);
    let finish_offset = generic_response_builder.finish();
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

pub struct WritableFlatBuffer<'a> {
    buffer: FlatBufferBuilder<'a>,
}

impl<'a> WritableFlatBuffer<'a> {
    pub fn new(buffer: FlatBufferBuilder<'a>) -> WritableFlatBuffer<'a> {
        WritableFlatBuffer { buffer }
    }

    pub fn into_buffer(self) -> FlatBufferBuilder<'a> {
        self.buffer
    }
}

impl<'a> AsRef<[u8]> for WritableFlatBuffer<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.finished_data()
    }
}

pub fn fuse_allow_other_enabled() -> io::Result<bool> {
    let file = File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        if line?.trim_start().starts_with("user_allow_other") {
            return Ok(true);
        }
    }
    Ok(false)
}
