use crate::base::message_types::ErrorCode;
use crate::base::FlatBufferResponse;
use crate::generated::*;
use crate::storage::local::FileStorage;
use flatbuffers::FlatBufferBuilder;

pub fn commit_write<'a, 'b>(
    request: GenericRequest<'a>,
    file_storage: &FileStorage,
    builder: FlatBufferBuilder<'b>,
) -> Result<FlatBufferResponse<'b>, ErrorCode> {
    match request.request_type() {
        RequestType::HardlinkIncrementRequest => {
            let hardlink_increment_request = request
                .request_as_hardlink_increment_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.hardlink_stage0_link_increment(hardlink_increment_request.inode(), builder)
        }
        RequestType::HardlinkRollbackRequest => {
            let hardlink_rollback_request = request
                .request_as_hardlink_rollback_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.hardlink_rollback(
                hardlink_rollback_request.inode(),
                *hardlink_rollback_request.last_modified_time(),
                builder,
            )
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
                builder,
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
                builder,
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
                builder,
            )
        }
        RequestType::UpdateParentRequest => {
            let update_parent_request = request
                .request_as_update_parent_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.update_parent(
                update_parent_request.inode(),
                update_parent_request.new_parent(),
                builder,
            )
        }
        RequestType::UpdateMetadataChangedTimeRequest => {
            let update_request = request
                .request_as_update_metadata_changed_time_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.update_metadata_changed_time(update_request.inode(), builder)
        }
        RequestType::DecrementInodeRequest => {
            let decrement_inode_request = request
                .request_as_decrement_inode_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.decrement_inode_link_count(
                decrement_inode_request.inode(),
                decrement_inode_request.decrement_count(),
                builder,
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
                builder,
            )
        }
        RequestType::LockRequest => {
            unreachable!("This should have been handled by the LockTable");
        }
        RequestType::UnlockRequest => {
            unreachable!("This should have been handled by the LockTable");
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
                builder,
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
                builder,
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
                builder,
            )
        }
        RequestType::FsyncRequest => {
            let fsync_request = request
                .request_as_fsync_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.fsync(fsync_request.inode(), builder)
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
                builder,
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
                builder,
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
                builder,
            )
        }
        RequestType::UtimensRequest => {
            let utimens_request = request
                .request_as_utimens_request()
                .ok_or(ErrorCode::BadRequest)?;
            file_storage.utimens(
                utimens_request.inode(),
                utimens_request.atime(),
                utimens_request.mtime(),
                *utimens_request.context(),
                builder,
            )
        }
        RequestType::MkdirRequest => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        RequestType::LookupRequest => unreachable!(),
        RequestType::ReadRequest => unreachable!(),
        RequestType::ReadRawRequest => unreachable!(),
        RequestType::ReaddirRequest => unreachable!(),
        RequestType::GetattrRequest => unreachable!(),
        RequestType::GetXattrRequest => unreachable!(),
        RequestType::RaftRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }
}
