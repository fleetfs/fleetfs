use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::generated::*;
use crate::storage::data_storage::BLOCK_SIZE;
use crate::storage::metadata_storage::InodeAttributes;
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
        RequestType::GetattrRequest => false,
        RequestType::FilesystemCheckRequest => false,
        RequestType::FilesystemChecksumRequest => false,
        RequestType::LookupRequest => false,
        RequestType::CreateRequest => false,
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
        RequestType::GetattrRequest => false,
        RequestType::FilesystemCheckRequest => false,
        RequestType::FilesystemChecksumRequest => false,
        RequestType::LookupRequest => false,
        RequestType::GetXattrRequest => false,
        RequestType::ListXattrsRequest => false,
        RequestType::SetXattrRequest => true,
        RequestType::RemoveXattrRequest => true,
        RequestType::CreateRequest => true,
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

pub fn to_fileattr_response(
    mut builder: FlatBufferBuilder,
    attributes: InodeAttributes,
) -> ResultResponse {
    let mut response_builder = FileMetadataResponseBuilder::new(&mut builder);
    response_builder.add_inode(attributes.inode);
    response_builder.add_size_bytes(attributes.size);
    response_builder.add_size_blocks(attributes.size / BLOCK_SIZE);
    response_builder.add_last_access_time(&attributes.last_accessed);
    response_builder.add_last_modified_time(&attributes.last_modified);
    response_builder.add_last_metadata_modified_time(&attributes.last_metadata_changed);
    response_builder.add_kind(attributes.kind);
    response_builder.add_mode(attributes.mode);
    response_builder.add_hard_links(attributes.hardlinks);
    response_builder.add_user_id(attributes.uid);
    response_builder.add_group_id(attributes.gid);
    response_builder.add_device_id(0); // TODO

    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::FileMetadataResponse, offset));
}

pub fn check_access(
    file_uid: u32,
    file_gid: u32,
    file_mode: u16,
    uid: u32,
    gid: u32,
    mut access_mask: u32,
) -> bool {
    // F_OK tests for existence of file
    if access_mask == libc::F_OK as u32 {
        return true;
    }

    // Process "other" permissions
    let file_mode = u32::from(file_mode);
    access_mask -= access_mask & file_mode;
    if gid == file_gid {
        access_mask -= access_mask & (file_mode >> 3);
    }
    if uid == file_uid {
        access_mask -= access_mask & (file_mode >> 6);
    }

    return access_mask == 0;
}
