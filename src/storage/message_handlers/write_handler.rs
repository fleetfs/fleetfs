use crate::base::message_types::{ArchivedRkyvRequest, ErrorCode, RkyvGenericResponse};
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
        ArchivedRkyvRequest::Utimens {
            inode,
            atime,
            mtime,
            context,
        } => file_storage.utimens(
            inode.into(),
            atime.as_ref().map(|x| x.into()),
            mtime.as_ref().map(|x| x.into()),
            context.into(),
        ),
        ArchivedRkyvRequest::SetXattr {
            inode,
            key,
            value,
            context,
        } => file_storage.set_xattr(inode.into(), key.as_str(), value, context.into()),
        ArchivedRkyvRequest::RemoveXattr {
            inode,
            key,
            context,
        } => file_storage.remove_xattr(inode.into(), key.as_str(), context.into()),
        ArchivedRkyvRequest::Mkdir { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        ArchivedRkyvRequest::Hardlink { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        ArchivedRkyvRequest::Rename { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        ArchivedRkyvRequest::Create { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        ArchivedRkyvRequest::Unlink { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        ArchivedRkyvRequest::Rmdir { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        ArchivedRkyvRequest::Chmod {
            inode,
            mode,
            context,
        } => file_storage.chmod(inode.into(), mode.into(), context.into()),
        ArchivedRkyvRequest::Chown {
            inode,
            uid,
            gid,
            context,
        } => file_storage.chown(
            inode.into(),
            uid.as_ref().map(|x| x.into()),
            gid.as_ref().map(|x| x.into()),
            context.into(),
        ),
        ArchivedRkyvRequest::Truncate {
            inode,
            new_length,
            context,
        } => file_storage.truncate(inode.into(), new_length.into(), context.into()),
        ArchivedRkyvRequest::Write {
            inode,
            offset,
            data,
        } => file_storage.write(inode.into(), offset.into(), data),
        ArchivedRkyvRequest::RemoveLink {
            parent,
            name,
            link_inode_and_uid,
            context,
            ..
        } => file_storage.remove_link(
            parent.into(),
            name.as_str(),
            link_inode_and_uid
                .as_ref()
                .map(|x| (x.inode.into(), x.uid.into())),
            context.into(),
        ),
        ArchivedRkyvRequest::ReplaceLink {
            parent,
            name,
            new_inode,
            kind,
            context,
            ..
        } => file_storage.replace_link(
            parent.into(),
            name.as_str(),
            new_inode.into(),
            kind.into(),
            context.into(),
        ),
        ArchivedRkyvRequest::CreateLink {
            inode,
            parent,
            name,
            kind,
            context,
            ..
        } => file_storage.create_link(
            inode.into(),
            parent.into(),
            name.as_str(),
            context.into(),
            kind.into(),
        ),
        ArchivedRkyvRequest::CreateInode {
            parent,
            uid,
            gid,
            mode,
            kind,
            ..
        } => file_storage.create_inode(
            parent.into(),
            uid.into(),
            gid.into(),
            mode.into(),
            kind.into(),
        ),
        ArchivedRkyvRequest::HardlinkIncrement { inode } => {
            file_storage.hardlink_stage0_link_increment(inode.into())
        }
        ArchivedRkyvRequest::UpdateParent {
            inode, new_parent, ..
        } => file_storage.update_parent(inode.into(), new_parent.into()),
        ArchivedRkyvRequest::UpdateMetadataChangedTime { inode, .. } => {
            file_storage.update_metadata_changed_time(inode.into())
        }
        ArchivedRkyvRequest::DecrementInode {
            inode,
            decrement_count,
            ..
        } => file_storage.decrement_inode_link_count(inode.into(), decrement_count.into()),
        ArchivedRkyvRequest::Lock { .. } => {
            unreachable!("This should have been handled by the LockTable");
        }
        ArchivedRkyvRequest::Unlock { .. } => {
            unreachable!("This should have been handled by the LockTable");
        }
        ArchivedRkyvRequest::FilesystemReady
        | ArchivedRkyvRequest::Flatbuffer(_)
        | ArchivedRkyvRequest::FilesystemInformation
        | ArchivedRkyvRequest::FilesystemChecksum
        | ArchivedRkyvRequest::FilesystemCheck
        | ArchivedRkyvRequest::Lookup { .. }
        | ArchivedRkyvRequest::GetAttr { .. }
        | ArchivedRkyvRequest::ListDir { .. }
        | ArchivedRkyvRequest::ListXattrs { .. }
        | ArchivedRkyvRequest::GetXattr { .. }
        | ArchivedRkyvRequest::LatestCommit { .. }
        | ArchivedRkyvRequest::RaftGroupLeader { .. }
        | ArchivedRkyvRequest::RaftMessage { .. } => {
            unreachable!()
        }
    }
}
