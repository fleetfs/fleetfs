use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::base::ResultResponse;
use crate::generated::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};

pub fn empty_response(mut buffer: FlatBufferBuilder) -> ResultResponse {
    let response_builder = EmptyResponseBuilder::new(&mut buffer);
    let offset = response_builder.finish().as_union_value();
    return Ok((buffer, ResponseType::EmptyResponse, offset));
}

pub fn finalize_request_without_prefix(
    builder: &mut FlatBufferBuilder,
    request_type: RequestType,
    finish_offset: WIPOffset<UnionWIPOffset>,
) {
    let mut generic_request_builder = GenericRequestBuilder::new(builder);
    generic_request_builder.add_request_type(request_type);
    generic_request_builder.add_request(finish_offset);
    let finish_offset = generic_request_builder.finish();
    builder.finish(finish_offset, None);
}

pub fn finalize_response_without_prefix(
    builder: &mut FlatBufferBuilder,
    response_type: ResponseType,
    finish_offset: WIPOffset<UnionWIPOffset>,
) {
    let mut generic_response_builder = GenericResponseBuilder::new(builder);
    generic_response_builder.add_response_type(response_type);
    generic_response_builder.add_response(finish_offset);
    let finish_offset = generic_response_builder.finish();
    builder.finish(finish_offset, None);
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
