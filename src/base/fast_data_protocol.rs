/*
 * A faster encoding for serializing large amounts of data. Used by the Read method to avoid the extra
 * copy that would be required to write an Rkyv
 */

use crate::base::{FlatBufferWithResponse, LengthPrefixedVec};
use crate::ErrorCode;
use flatbuffers::FlatBufferBuilder;

fn serialize_error(error_code: ErrorCode) -> u8 {
    match error_code {
        ErrorCode::DoesNotExist => 0,
        ErrorCode::InodeDoesNotExist => 1,
        ErrorCode::FileTooLarge => 2,
        ErrorCode::AccessDenied => 3,
        ErrorCode::OperationNotPermitted => 4,
        ErrorCode::AlreadyExists => 5,
        ErrorCode::NameTooLong => 6,
        ErrorCode::NotEmpty => 7,
        ErrorCode::MissingXattrKey => 8,
        ErrorCode::BadResponse => 9,
        ErrorCode::BadRequest => 10,
        ErrorCode::Corrupted => 11,
        ErrorCode::RaftFailure => 12,
        ErrorCode::InvalidXattrNamespace => 13,
        ErrorCode::Uncategorized => 14,
    }
}

fn deserialize_error(value: u8) -> ErrorCode {
    match value {
        0 => ErrorCode::DoesNotExist,
        1 => ErrorCode::InodeDoesNotExist,
        2 => ErrorCode::FileTooLarge,
        3 => ErrorCode::AccessDenied,
        4 => ErrorCode::OperationNotPermitted,
        5 => ErrorCode::AlreadyExists,
        6 => ErrorCode::NameTooLong,
        7 => ErrorCode::NotEmpty,
        8 => ErrorCode::MissingXattrKey,
        9 => ErrorCode::BadResponse,
        10 => ErrorCode::BadRequest,
        11 => ErrorCode::Corrupted,
        12 => ErrorCode::RaftFailure,
        13 => ErrorCode::InvalidXattrNamespace,
        14 => ErrorCode::Uncategorized,
        _ => unreachable!(),
    }
}

pub fn to_fast_read_response(
    builder: FlatBufferBuilder,
    response: Result<LengthPrefixedVec, ErrorCode>,
) -> FlatBufferWithResponse {
    match response {
        Ok(mut data) => {
            data.push(0);
            // No error
            data.push(0);
            FlatBufferWithResponse::with_separate_response(builder, data)
        }
        Err(error_code) => {
            let mut data = LengthPrefixedVec::zeros(0);
            data.push(serialize_error(error_code));
            // Error occurred
            data.push(1);
            FlatBufferWithResponse::with_separate_response(builder, data)
        }
    }
}

pub fn decode_fast_read_response_inplace(response: &mut Vec<u8>) -> Result<&Vec<u8>, ErrorCode> {
    let is_error = response.pop().unwrap();
    assert!(is_error <= 1);
    let error_value = response.pop().unwrap();
    if is_error == 0 {
        Ok(response)
    } else {
        Err(deserialize_error(error_value))
    }
}
