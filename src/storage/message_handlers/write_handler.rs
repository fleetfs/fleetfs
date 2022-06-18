use crate::base::fb_into_timestamp;
use crate::base::message_types::{ArchivedRkyvRequest, ErrorCode, RkyvGenericResponse};
use crate::generated::*;
use crate::storage::local::FileStorage;

pub fn commit_write(
    request: &ArchivedRkyvRequest,
    file_storage: &FileStorage,
) -> Result<RkyvGenericResponse, ErrorCode> {
    match request {
        ArchivedRkyvRequest::Fsync { inode } => file_storage.fsync(inode.into()),
        ArchivedRkyvRequest::HardlinkRollback {
            inode,
            last_modified_time,
        } => file_storage.hardlink_rollback(inode.into(), last_modified_time.into()),
        ArchivedRkyvRequest::Flatbuffer(data) => {
            let request = get_root_as_generic_request(data);
            commit_write_flatbuffer(request, file_storage)
        }
        ArchivedRkyvRequest::Lock { .. } => {
            unreachable!("This should have been handled by the LockTable");
        }
        ArchivedRkyvRequest::Unlock { .. } => {
            unreachable!("This should have been handled by the LockTable");
        }
        ArchivedRkyvRequest::FilesystemReady
        | ArchivedRkyvRequest::FilesystemInformation
        | ArchivedRkyvRequest::FilesystemChecksum
        | ArchivedRkyvRequest::FilesystemCheck
        | ArchivedRkyvRequest::GetAttr { .. }
        | ArchivedRkyvRequest::ListDir { .. }
        | ArchivedRkyvRequest::ListXattrs { .. }
        | ArchivedRkyvRequest::LatestCommit { .. }
        | ArchivedRkyvRequest::RaftGroupLeader { .. }
        | ArchivedRkyvRequest::RaftMessage { .. } => {
            unreachable!()
        }
    }
}

pub fn commit_write_flatbuffer(
    request: GenericRequest,
    file_storage: &FileStorage,
) -> Result<RkyvGenericResponse, ErrorCode> {
    match request.request_type() {
        RequestType::HardlinkIncrementRequest => {
            let hardlink_increment_request = request
                .request_as_hardlink_increment_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.hardlink_stage0_link_increment(hardlink_increment_request.inode())
        }
        RequestType::CreateInodeRequest => {
            let create_inode_request = request
                .request_as_create_inode_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.create_inode(
                create_inode_request.parent(),
                create_inode_request.uid(),
                create_inode_request.gid(),
                create_inode_request.mode(),
                create_inode_request.kind(),
            )
        }
        RequestType::CreateLinkRequest => {
            let create_link_request = request
                .request_as_create_link_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.create_link(
                create_link_request.inode(),
                create_link_request.parent(),
                create_link_request.name(),
                *create_link_request.context(),
                create_link_request.kind(),
            )
        }
        RequestType::ReplaceLinkRequest => {
            let replace_link_request = request
                .request_as_replace_link_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.replace_link(
                replace_link_request.parent(),
                replace_link_request.name(),
                replace_link_request.new_inode(),
                replace_link_request.kind(),
                *replace_link_request.context(),
            )
        }
        RequestType::UpdateParentRequest => {
            let update_parent_request = request
                .request_as_update_parent_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.update_parent(
                update_parent_request.inode(),
                update_parent_request.new_parent(),
            )
        }
        RequestType::UpdateMetadataChangedTimeRequest => {
            let update_request = request
                .request_as_update_metadata_changed_time_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.update_metadata_changed_time(update_request.inode())
        }
        RequestType::DecrementInodeRequest => {
            let decrement_inode_request = request
                .request_as_decrement_inode_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.decrement_inode_link_count(
                decrement_inode_request.inode(),
                decrement_inode_request.decrement_count(),
            )
        }
        RequestType::RemoveLinkRequest => {
            let remove_link_request = request
                .request_as_remove_link_request()
                .ok_or(ErrorCode::BadRequest)?;
            let link_inode_and_uid = if let Some(inode) = remove_link_request.link_inode() {
                let uid = remove_link_request.link_uid().unwrap();
                Some((inode.value(), uid.value()))
            } else {
                None
            };
            file_storage.remove_link(
                remove_link_request.parent(),
                remove_link_request.name(),
                link_inode_and_uid,
                *remove_link_request.context(),
            )
        }
        RequestType::HardlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RenameRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::ChmodRequest => {
            let chmod_request = request
                .request_as_chmod_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.chmod(
                chmod_request.inode(),
                chmod_request.mode(),
                *chmod_request.context(),
            )
        }
        RequestType::ChownRequest => {
            let chown_request = request
                .request_as_chown_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.chown(
                chown_request.inode(),
                chown_request.uid().map(OptionalUInt::value),
                chown_request.gid().map(OptionalUInt::value),
                *chown_request.context(),
            )
        }
        RequestType::TruncateRequest => {
            let truncate_request = request
                .request_as_truncate_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.truncate(
                truncate_request.inode(),
                truncate_request.new_length(),
                *truncate_request.context(),
            )
        }
        RequestType::CreateRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::SetXattrRequest => {
            let set_xattr_request = request
                .request_as_set_xattr_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.set_xattr(
                set_xattr_request.inode(),
                set_xattr_request.key(),
                set_xattr_request.value(),
                *set_xattr_request.context(),
            )
        }
        RequestType::RemoveXattrRequest => {
            let remove_xattr_request = request
                .request_as_remove_xattr_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.remove_xattr(
                remove_xattr_request.inode(),
                remove_xattr_request.key(),
                *remove_xattr_request.context(),
            )
        }
        RequestType::UnlinkRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::RmdirRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::WriteRequest => {
            let write_request = request
                .request_as_write_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.write(
                write_request.inode(),
                write_request.offset(),
                write_request.data(),
            )
        }
        RequestType::UtimensRequest => {
            let utimens_request = request
                .request_as_utimens_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.utimens(
                utimens_request.inode(),
                utimens_request.atime().map(fb_into_timestamp),
                utimens_request.mtime().map(fb_into_timestamp),
                *utimens_request.context(),
            )
        }
        RequestType::MkdirRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::LookupRequest => unreachable!(),
        RequestType::ReadRequest => unreachable!(),
        RequestType::ReadRawRequest => unreachable!(),
        RequestType::GetXattrRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }
}
