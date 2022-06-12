use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use crate::base::message_types::RkyvGenericResponse;
use crate::base::ResultResponse;
use crate::generated::*;
use crate::ErrorCode;
use rkyv::AlignedVec;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};

pub fn empty_response(mut builder: FlatBufferBuilder) -> ResultResponse {
    let rkyv_response = RkyvGenericResponse::Empty;
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::RkyvResponse, response_offset));
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
    if response.response_type() == ResponseType::RkyvResponse {
        let rkyv_data = response.response_as_rkyv_response().unwrap().rkyv_data();
        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        if let Some(error_code) = rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned)
            .unwrap()
            .as_error_response()
        {
            return Err(error_code);
        }
    }
    return Ok(response);
}

pub fn check_access(
    file_uid: u32,
    file_gid: u32,
    file_mode: u16,
    uid: u32,
    gid: u32,
    mut access_mask: i32,
) -> bool {
    // F_OK tests for existence of file
    if access_mask == libc::F_OK {
        return true;
    }
    let file_mode = i32::from(file_mode);

    // root is allowed to read & write anything
    if uid == 0 {
        // root only allowed to exec if one of the X bits is set
        access_mask &= libc::X_OK;
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
