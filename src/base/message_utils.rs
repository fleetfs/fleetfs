use crate::generated::*;

pub struct RequestMetaInfo {
    pub raft_group: Option<u16>,
    pub inode: Option<u64>, // Some if the request accesses a single inode (i.e. None for Rename)
    pub lock_id: Option<u64>,
    pub access_type: AccessType, // Used to determine locks to acquire
    pub distribution_requirement: DistributionRequirement,
}

pub enum AccessType {
    ReadData,
    ReadMetadata,
    LockMetadata,
    WriteMetadata,
    WriteDataAndMetadata,
    NoAccess,
}

// Where this message can be processed
pub enum DistributionRequirement {
    Any,                    // Any node can process this message
    TransactionCoordinator, // Any node can process this message by acting as a transcation coordinator
    RaftGroup,              // Must be processed by a specific rgroup
    Node,                   // Must be processed by a specific node
}

pub fn flatbuffer_request_meta_info(request: &GenericRequest<'_>) -> RequestMetaInfo {
    match request.request_type() {
        RequestType::ReadRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_read_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::ReadData,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::ReadRawRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_read_raw_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::ReadData,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::SetXattrRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_set_xattr_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::RemoveXattrRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_remove_xattr_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::UnlinkRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_unlink_request().unwrap().parent()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::RmdirRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_rmdir_request().unwrap().parent()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::WriteRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_write_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteDataAndMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::UtimensRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_utimens_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::ChmodRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_chmod_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::ChownRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_chown_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::TruncateRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_truncate_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::WriteDataAndMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::FsyncRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_fsync_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::NoAccess,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::MkdirRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_mkdir_request().unwrap().parent()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::TransactionCoordinator,
        },
        RequestType::CreateRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_create_request().unwrap().parent()),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::TransactionCoordinator,
        },
        RequestType::LockRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_lock_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::LockMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::UnlockRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_unlock_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::NoAccess,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::HardlinkIncrementRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(
                request
                    .request_as_hardlink_increment_request()
                    .unwrap()
                    .inode(),
            ),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::HardlinkRollbackRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(
                request
                    .request_as_hardlink_rollback_request()
                    .unwrap()
                    .inode(),
            ),
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::CreateInodeRequest => RequestMetaInfo {
            raft_group: Some(
                request
                    .request_as_create_inode_request()
                    .unwrap()
                    .raft_group(),
            ),
            inode: None,
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::DecrementInodeRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(
                request
                    .request_as_decrement_inode_request()
                    .unwrap()
                    .inode(),
            ),
            lock_id: request
                .request_as_decrement_inode_request()
                .unwrap()
                .lock_id()
                .map(|x| x.value()),
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::RemoveLinkRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_remove_link_request().unwrap().parent()),
            lock_id: request
                .request_as_remove_link_request()
                .unwrap()
                .lock_id()
                .map(|x| x.value()),
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::CreateLinkRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_create_link_request().unwrap().parent()),
            lock_id: request
                .request_as_create_link_request()
                .unwrap()
                .lock_id()
                .map(|x| x.value()),
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::ReplaceLinkRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_replace_link_request().unwrap().parent()),
            lock_id: request
                .request_as_replace_link_request()
                .unwrap()
                .lock_id()
                .map(|x| x.value()),
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::UpdateParentRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_update_parent_request().unwrap().inode()),
            lock_id: request
                .request_as_update_parent_request()
                .unwrap()
                .lock_id()
                .map(|x| x.value()),
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::UpdateMetadataChangedTimeRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(
                request
                    .request_as_update_metadata_changed_time_request()
                    .unwrap()
                    .inode(),
            ),
            lock_id: request
                .request_as_update_metadata_changed_time_request()
                .unwrap()
                .lock_id()
                .map(|x| x.value()),
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::HardlinkRequest => RequestMetaInfo {
            raft_group: None,
            inode: None,
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::TransactionCoordinator,
        },
        RequestType::RenameRequest => RequestMetaInfo {
            raft_group: None,
            inode: None,
            lock_id: None,
            access_type: AccessType::WriteMetadata,
            distribution_requirement: DistributionRequirement::TransactionCoordinator,
        },
        RequestType::LookupRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_lookup_request().unwrap().parent()),
            lock_id: None,
            access_type: AccessType::ReadMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::GetXattrRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_get_xattr_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::ReadMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::ReaddirRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_readdir_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::ReadMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::GetattrRequest => RequestMetaInfo {
            raft_group: None,
            inode: Some(request.request_as_getattr_request().unwrap().inode()),
            lock_id: None,
            access_type: AccessType::ReadMetadata,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::RaftRequest => RequestMetaInfo {
            raft_group: Some(request.request_as_raft_request().unwrap().raft_group()),
            inode: None,
            lock_id: None,
            access_type: AccessType::NoAccess,
            distribution_requirement: DistributionRequirement::RaftGroup,
        },
        RequestType::NONE => unreachable!(),
    }
}

// Locks held by the request
pub fn request_locks(request: &GenericRequest<'_>) -> Option<u64> {
    flatbuffer_request_meta_info(request).lock_id
}

pub fn accessed_inode(request: &GenericRequest<'_>) -> Option<u64> {
    flatbuffer_request_meta_info(request).inode
}

pub fn raft_group(request: &GenericRequest) -> Option<u16> {
    flatbuffer_request_meta_info(request).raft_group
}

pub fn access_type(request: &GenericRequest) -> AccessType {
    flatbuffer_request_meta_info(request).access_type
}

pub fn distribution_requirement(request: &GenericRequest) -> DistributionRequirement {
    flatbuffer_request_meta_info(request).distribution_requirement
}
