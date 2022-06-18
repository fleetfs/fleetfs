use crate::base::message_types::ArchivedRkyvRequest;
use crate::base::message_types::{ErrorCode, RkyvGenericResponse};
use crate::base::{finalize_response, FlatBufferWithResponse};
use crate::base::{flatbuffer_request_meta_info, LocalContext, RequestMetaInfo};
use crate::base::{DistributionRequirement, ResultResponse};
use crate::client::RemoteRaftGroups;
use crate::generated::*;
use crate::storage::message_handlers::fsck_handler::{checksum_request, fsck};
use crate::storage::message_handlers::router::FullOrPartialResponse::{Full, Partial};
use crate::storage::message_handlers::transaction_coordinator::{
    create_transaction, hardlink_transaction, rename_transaction, rmdir_transaction,
    unlink_transaction,
};
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::storage::raft_node::sync_with_leader;
use flatbuffers::FlatBufferBuilder;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::sync::Arc;

enum FullOrPartialResponse {
    Full(FlatBufferWithResponse<'static>),
    Partial(RkyvGenericResponse),
}

pub fn rkyv_response_to_fb(
    mut builder: FlatBufferBuilder,
    rkyv_response: RkyvGenericResponse,
) -> ResultResponse {
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::RkyvResponse, offset));
}

pub fn to_error_response(
    mut builder: FlatBufferBuilder,
    error_code: ErrorCode,
) -> FlatBufferBuilder {
    let rkyv_response = RkyvGenericResponse::ErrorOccurred(error_code);
    let rkyv_bytes = rkyv::to_bytes::<_, 8>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let offset = response_builder.finish().as_union_value();
    finalize_response(&mut builder, ResponseType::RkyvResponse, offset);
    return builder;
}

async fn request_router_inner(
    request: GenericRequest<'_>,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    _context: LocalContext,
    builder: FlatBufferBuilder<'static>,
) -> Result<FullOrPartialResponse, ErrorCode> {
    match request.request_type() {
        RequestType::ReadRequest => {
            if let Some(read_request) = request.request_as_read_request() {
                let inode = read_request.inode();
                let offset = read_request.offset();
                let read_size = read_request.read_size();
                let latest_commit = raft
                    .lookup_by_inode(inode)
                    .get_latest_commit_from_leader()
                    .await?;
                raft.lookup_by_inode(inode).sync(latest_commit).await?;
                return raft
                    .lookup_by_inode(inode)
                    .file_storage()
                    // TODO: Use the real term, not zero
                    .read(
                        inode,
                        offset,
                        read_size,
                        CommitId::new(0, latest_commit),
                        builder,
                    )
                    .await
                    .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReadRawRequest => {
            if let Some(read_request) = request.request_as_read_raw_request() {
                let inode = read_request.inode();
                raft.lookup_by_inode(inode)
                    .sync(read_request.required_commit().index())
                    .await?;
                return Ok(Full(raft.lookup_by_inode(inode).file_storage().read_raw(
                    inode,
                    read_request.offset(),
                    read_request.read_size(),
                    builder,
                )));
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::SetXattrRequest => {
            if let Some(set_xattr_request) = request.request_as_set_xattr_request() {
                return raft
                    .lookup_by_inode(set_xattr_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RemoveXattrRequest => {
            if let Some(remove_xattr_request) = request.request_as_remove_xattr_request() {
                return raft
                    .lookup_by_inode(remove_xattr_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UnlinkRequest => {
            if let Some(unlink_request) = request.request_as_unlink_request() {
                return unlink_transaction(
                    unlink_request.parent(),
                    unlink_request.name().to_string(),
                    *unlink_request.context(),
                    builder,
                    raft.clone(),
                    remote_rafts.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RmdirRequest => {
            if let Some(rmdir_request) = request.request_as_rmdir_request() {
                return rmdir_transaction(
                    rmdir_request.parent(),
                    rmdir_request.name().to_string(),
                    *rmdir_request.context(),
                    builder,
                    raft.clone(),
                    remote_rafts.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::WriteRequest => {
            if let Some(write_request) = request.request_as_write_request() {
                return raft
                    .lookup_by_inode(write_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UtimensRequest => {
            if let Some(utimens_request) = request.request_as_utimens_request() {
                return raft
                    .lookup_by_inode(utimens_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ChmodRequest => {
            if let Some(chmod_request) = request.request_as_chmod_request() {
                return raft
                    .lookup_by_inode(chmod_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ChownRequest => {
            if let Some(chown_request) = request.request_as_chown_request() {
                return raft
                    .lookup_by_inode(chown_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::TruncateRequest => {
            if let Some(truncate_request) = request.request_as_truncate_request() {
                return raft
                    .lookup_by_inode(truncate_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::FsyncRequest => {
            if let Some(fsync_request) = request.request_as_fsync_request() {
                return raft
                    .lookup_by_inode(fsync_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::MkdirRequest => {
            if let Some(mkdir_request) = request.request_as_mkdir_request() {
                return create_transaction(
                    mkdir_request.parent(),
                    mkdir_request.name().to_string(),
                    mkdir_request.uid(),
                    mkdir_request.gid(),
                    mkdir_request.mode(),
                    FileKind::Directory,
                    builder,
                    raft.clone(),
                    remote_rafts.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::CreateRequest => {
            if let Some(create_request) = request.request_as_create_request() {
                return create_transaction(
                    create_request.parent(),
                    create_request.name().to_string(),
                    create_request.uid(),
                    create_request.gid(),
                    create_request.mode(),
                    create_request.kind(),
                    builder,
                    raft.clone(),
                    remote_rafts.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LockRequest => {
            if let Some(lock_request) = request.request_as_lock_request() {
                return raft
                    .lookup_by_inode(lock_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UnlockRequest => {
            if let Some(unlock_request) = request.request_as_unlock_request() {
                return raft
                    .lookup_by_inode(unlock_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::HardlinkIncrementRequest => {
            // Internal request used during transaction processing
            if let Some(increment_request) = request.request_as_hardlink_increment_request() {
                return raft
                    .lookup_by_inode(increment_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::HardlinkRollbackRequest => {
            // Internal request used during transaction processing
            if let Some(rollback_request) = request.request_as_hardlink_rollback_request() {
                return raft
                    .lookup_by_inode(rollback_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::CreateInodeRequest => {
            // Internal request used during transaction processing
            if let Some(create_inode_request) = request.request_as_create_inode_request() {
                return raft
                    .lookup_by_raft_group(create_inode_request.raft_group())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::DecrementInodeRequest => {
            // Internal request used during transaction processing
            if let Some(decrement_request) = request.request_as_decrement_inode_request() {
                return raft
                    .lookup_by_inode(decrement_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RemoveLinkRequest => {
            // Internal request used during transaction processing
            if let Some(remove_request) = request.request_as_remove_link_request() {
                return raft
                    .lookup_by_inode(remove_request.parent())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReplaceLinkRequest => {
            // Internal request used during transaction processing
            if let Some(replace_request) = request.request_as_replace_link_request() {
                return raft
                    .lookup_by_inode(replace_request.parent())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UpdateParentRequest => {
            // Internal request used during transaction processing
            if let Some(update_request) = request.request_as_update_parent_request() {
                return raft
                    .lookup_by_inode(update_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UpdateMetadataChangedTimeRequest => {
            // Internal request used during transaction processing
            if let Some(update_request) = request.request_as_update_metadata_changed_time_request()
            {
                return raft
                    .lookup_by_inode(update_request.inode())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::CreateLinkRequest => {
            // Internal request used during transaction processing
            if let Some(create_link_request) = request.request_as_create_link_request() {
                return raft
                    .lookup_by_inode(create_link_request.parent())
                    .propose_flatbuffer(request)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::HardlinkRequest => {
            if let Some(hardlink_request) = request.request_as_hardlink_request() {
                return hardlink_transaction(
                    hardlink_request,
                    builder,
                    raft.clone(),
                    remote_rafts.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RenameRequest => {
            if let Some(rename_request) = request.request_as_rename_request() {
                return rename_transaction(
                    rename_request.parent(),
                    rename_request.name().to_string(),
                    rename_request.new_parent(),
                    rename_request.new_name().to_string(),
                    *rename_request.context(),
                    builder,
                    raft.clone(),
                    remote_rafts.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LookupRequest => {
            if let Some(lookup_request) = request.request_as_lookup_request() {
                let parent = lookup_request.parent();
                let name = lookup_request.name().to_string();
                let user_context = *lookup_request.context();
                sync_with_leader(raft.lookup_by_inode(parent)).await?;
                raft.lookup_by_inode(parent)
                    .file_storage()
                    .lookup(parent, &name, user_context)
                    .map(Partial)
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetXattrRequest => {
            if let Some(get_xattr_request) = request.request_as_get_xattr_request() {
                let inode = get_xattr_request.inode();
                let key = get_xattr_request.key().to_string();
                sync_with_leader(raft.lookup_by_inode(inode)).await?;
                raft.lookup_by_inode(inode)
                    .file_storage()
                    .get_xattr(inode, &key, *get_xattr_request.context())
                    .map(Partial)
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::NONE => unreachable!(),
    }
}

// Determines whether the request can be handled by the local node, or whether it needs to be
// forwarded to a different raft group
fn can_handle_locally(request_meta: RequestMetaInfo, local_rafts: &LocalRaftGroupManager) -> bool {
    match request_meta.distribution_requirement {
        DistributionRequirement::Any => true,
        DistributionRequirement::TransactionCoordinator => true,
        // TODO: check that this message was sent to the right node. At the moment, we assume the client handled that
        DistributionRequirement::Node => true,
        DistributionRequirement::RaftGroup => {
            if let Some(group) = request_meta.raft_group {
                local_rafts.has_raft_group(group)
            } else {
                local_rafts.inode_stored_locally(request_meta.inode.unwrap())
            }
        }
    }
}

async fn forward_request(
    request: &ArchivedRkyvRequest,
    builder: FlatBufferBuilder<'static>,
    rafts: Arc<RemoteRaftGroups>,
) -> FlatBufferWithResponse<'static> {
    if let Ok(response) = rafts.forward_request(request).await {
        FlatBufferWithResponse::with_separate_response(builder, response)
    } else {
        FlatBufferWithResponse::new(to_error_response(
            FlatBufferBuilder::new(),
            ErrorCode::Uncategorized,
        ))
    }
}

async fn forward_flatbuffer_request(
    request: &GenericRequest<'_>,
    builder: FlatBufferBuilder<'static>,
    rafts: Arc<RemoteRaftGroups>,
) -> FlatBufferWithResponse<'static> {
    if let Ok(response) = rafts.forward_flatbuffer_request(request).await {
        FlatBufferWithResponse::with_separate_response(builder, response)
    } else {
        FlatBufferWithResponse::new(to_error_response(
            FlatBufferBuilder::new(),
            ErrorCode::Uncategorized,
        ))
    }
}

pub async fn request_router(
    request: &ArchivedRkyvRequest,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    context: LocalContext,
    builder: FlatBufferBuilder<'static>,
) -> FlatBufferWithResponse<'static> {
    match request {
        ArchivedRkyvRequest::Flatbuffer(flatbuffer) => {
            let request = get_root_as_generic_request(flatbuffer);
            let response = flatbuffer_request_router(
                request,
                raft.clone(),
                remote_rafts.clone(),
                context.clone(),
                builder,
            )
            .await;
            response
        }
        _ => rkyv_request_router(request, raft, remote_rafts, context, builder).await,
    }
}

async fn rkyv_request_router<'a>(
    request: &ArchivedRkyvRequest,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    context: LocalContext,
    mut builder: FlatBufferBuilder<'static>,
) -> FlatBufferWithResponse<'static> {
    if !can_handle_locally(request.meta_info(), &raft) {
        return forward_request(request, builder, remote_rafts.clone()).await;
    }

    match rkyv_request_router_inner(request, raft, remote_rafts, context).await {
        Ok(rkyv_response) => {
            let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
            let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
            let mut response_builder = RkyvResponseBuilder::new(&mut builder);
            response_builder.add_rkyv_data(flatbuffer_offset);
            let offset = response_builder.finish().as_union_value();
            finalize_response(&mut builder, ResponseType::RkyvResponse, offset);
            FlatBufferWithResponse::new(builder)
        }
        Err(error_code) => {
            FlatBufferWithResponse::new(to_error_response(FlatBufferBuilder::new(), error_code))
        }
    }
}

async fn rkyv_request_router_inner(
    request: &ArchivedRkyvRequest,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    context: LocalContext,
) -> Result<RkyvGenericResponse, ErrorCode> {
    match request {
        ArchivedRkyvRequest::FilesystemReady => {
            for node in raft.all_groups() {
                node.get_leader().await?;
            }
            // Ensure that all other nodes are ready too
            remote_rafts
                .wait_for_ready()
                .await
                .map_err(|_| ErrorCode::Uncategorized)?;

            Ok(RkyvGenericResponse::Empty)
        }
        ArchivedRkyvRequest::FilesystemInformation => {
            Ok(raft.all_groups().next().unwrap().file_storage().statfs())
        }
        ArchivedRkyvRequest::FilesystemCheck => fsck(context.clone(), raft.clone()).await,
        ArchivedRkyvRequest::FilesystemChecksum => checksum_request(raft.clone()).await,
        ArchivedRkyvRequest::GetAttr { inode } => {
            let inode = inode.into();
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode).file_storage().getattr(inode)
        }
        ArchivedRkyvRequest::ListDir { inode } => {
            let inode = inode.into();
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode).file_storage().readdir(inode)
        }
        ArchivedRkyvRequest::ListXattrs { inode } => {
            let inode: u64 = inode.into();
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode)
                .file_storage()
                .list_xattrs(inode)
        }
        ArchivedRkyvRequest::LatestCommit { raft_group } => {
            let index = raft
                .lookup_by_raft_group(raft_group.into())
                .get_latest_local_commit();
            Ok(RkyvGenericResponse::LatestCommit { term: 0, index })
        }
        ArchivedRkyvRequest::RaftGroupLeader { raft_group } => {
            let rgroup = raft.lookup_by_raft_group(raft_group.into());
            let leader = rgroup.get_leader().await?;

            Ok(RkyvGenericResponse::NodeId { id: leader })
        }
        ArchivedRkyvRequest::RaftMessage { raft_group, data } => {
            let mut deserialized_message = Message::new();
            deserialized_message.merge_from_bytes(data).unwrap();
            raft.lookup_by_raft_group(raft_group.into())
                .apply_messages(&[deserialized_message])
                .unwrap();
            Ok(RkyvGenericResponse::Empty)
        }
        ArchivedRkyvRequest::Flatbuffer(_) => unreachable!(),
    }
}

async fn flatbuffer_request_router<'a>(
    request: GenericRequest<'a>,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    context: LocalContext,
    builder: FlatBufferBuilder<'static>,
) -> FlatBufferWithResponse<'static> {
    if !can_handle_locally(flatbuffer_request_meta_info(&request), &raft) {
        return forward_flatbuffer_request(&request, builder, remote_rafts.clone()).await;
    }

    match request_router_inner(request, raft, remote_rafts, context, builder).await {
        Ok(response) => match response {
            Full(full_response) => return full_response,
            Partial(response) => {
                let (mut builder, response_type, response_offset) =
                    rkyv_response_to_fb(FlatBufferBuilder::new(), response).unwrap();
                finalize_response(&mut builder, response_type, response_offset);
                return FlatBufferWithResponse::new(builder);
            }
        },
        Err(error_code) => {
            return FlatBufferWithResponse::new(to_error_response(
                FlatBufferBuilder::new(),
                error_code,
            ));
        }
    };
}
