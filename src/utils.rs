use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::generated::*;
use crate::storage::data_storage::BLOCK_SIZE;
use crate::storage::metadata_storage::InodeAttributes;
use byteorder::{ByteOrder, LittleEndian};
use flatbuffers::EndianScalar;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, ErrorKind};
use std::net::{IpAddr, SocketAddr};

pub type ResultResponse<'a> = Result<FlatBufferResponse<'a>, ErrorCode>;

pub type FlatBufferResponse<'a> = (
    FlatBufferBuilder<'a>,
    ResponseType,
    WIPOffset<UnionWIPOffset>,
);

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

pub fn decode_fast_read_response_inplace(response: &mut Vec<u8>) -> Result<&Vec<u8>, ErrorCode> {
    let value = response.pop().unwrap().from_little_endian() as i8;
    let p = &value as *const i8 as *const ErrorCode;
    let error_code = unsafe { *p };
    if error_code == ErrorCode::DefaultValueNotAnError {
        Ok(response)
    } else {
        Err(error_code)
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
    let file_mode = u32::from(file_mode);

    // root is allowed to read & write anything
    if uid == 0 {
        // root only allowed to exec if one of the X bits is set
        access_mask &= libc::X_OK as u32;
        access_mask -= access_mask & (file_mode >> 6);
        access_mask -= access_mask & (file_mode >> 3);
        access_mask -= access_mask & file_mode;
        return access_mask == 0;
    }

    if uid == file_uid {
        access_mask -= access_mask & (file_mode >> 6);
    } else if gid == file_gid {
        access_mask -= access_mask & (file_mode >> 3);
    } else {
        access_mask -= access_mask & file_mode;
    }

    return access_mask == 0;
}

pub struct LengthPrefixedVec {
    data: Vec<u8>,
}

impl LengthPrefixedVec {
    pub fn with_capacity(length: usize) -> LengthPrefixedVec {
        let mut data = Vec::with_capacity(length + 4);
        data.extend(vec![0; 4]);
        LengthPrefixedVec { data }
    }

    pub fn zeros(length: usize) -> LengthPrefixedVec {
        let mut data = vec![0; length + 4];
        LittleEndian::write_u32(&mut data, length as u32);
        LengthPrefixedVec { data }
    }

    pub fn length_prefixed_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[4..]
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[4..]
    }

    pub fn extend<'a, T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        self.data.extend(iter);
        let length = self.data.len() as u32;
        LittleEndian::write_u32(&mut self.data, length - 4);
    }

    pub fn truncate(&mut self, new_length: usize) {
        self.data.truncate(new_length + 4);
        let length = self.data.len() as u32;
        LittleEndian::write_u32(&mut self.data, length - 4);
    }

    pub fn push(&mut self, value: u8) {
        self.data.push(value);
        let length = self.data.len() as u32;
        LittleEndian::write_u32(&mut self.data, length - 4);
    }
}

pub fn node_id_from_address(address: &SocketAddr) -> u64 {
    let port = address.port();
    match address.ip() {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            u64::from(octets[0]) << 40
                | u64::from(octets[1]) << 32
                | u64::from(octets[2]) << 24
                | u64::from(octets[3]) << 16
                | u64::from(port)
        }
        IpAddr::V6(v6) => {
            // TODO: there could be collisions. Should be generated randomly and then dynamically discovered
            let mut hasher = DefaultHasher::new();
            v6.hash(&mut hasher);
            port.hash(&mut hasher);
            hasher.finish()
        }
    }
}
