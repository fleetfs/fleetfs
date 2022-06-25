use crate::base::message_types::ErrorCode;
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
            ErrorCode::Uncategorized
        }
        _ => ErrorCode::Uncategorized,
    }
}
