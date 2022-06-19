use crate::base::message_types::{ArchivedRkyvRequest, FileKind};
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
    _remote_rafts: Arc<RemoteRaftGroups>,
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
    if let Ok(response) = rafts.forward_archived_request(request).await {
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
        ArchivedRkyvRequest::HardlinkIncrement { inode }
        | ArchivedRkyvRequest::DecrementInode { inode, .. }
        | ArchivedRkyvRequest::UpdateParent { inode, .. }
        | ArchivedRkyvRequest::UpdateMetadataChangedTime { inode, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::Utimens { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::Chmod { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::Chown { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::Truncate { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::SetXattr { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::RemoveXattr { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::Unlink {
            parent,
            name,
            context,
        } => {
            unlink_transaction(
                parent.into(),
                name.as_str(),
                context.into(),
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        ArchivedRkyvRequest::Rmdir {
            parent,
            name,
            context,
        } => {
            rmdir_transaction(
                parent.into(),
                name.as_str(),
                context.into(),
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        ArchivedRkyvRequest::Mkdir {
            parent,
            name,
            uid,
            gid,
            mode,
        } => {
            create_transaction(
                parent.into(),
                name.as_str(),
                uid.into(),
                gid.into(),
                mode.into(),
                FileKind::Directory,
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        ArchivedRkyvRequest::Create {
            parent,
            name,
            uid,
            gid,
            mode,
            kind,
        } => {
            create_transaction(
                parent.into(),
                name.as_str(),
                uid.into(),
                gid.into(),
                mode.into(),
                kind.into(),
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        ArchivedRkyvRequest::Lookup {
            parent,
            name,
            context,
        } => {
            sync_with_leader(raft.lookup_by_inode(parent.into())).await?;
            raft.lookup_by_inode(parent.into()).file_storage().lookup(
                parent.into(),
                name.as_str(),
                context.into(),
            )
        }
        ArchivedRkyvRequest::GetXattr {
            inode,
            key,
            context,
        } => {
            sync_with_leader(raft.lookup_by_inode(inode.into())).await?;
            raft.lookup_by_inode(inode.into()).file_storage().get_xattr(
                inode.into(),
                key.as_str(),
                context.into(),
            )
        }
        ArchivedRkyvRequest::CreateInode { raft_group, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_raft_group(raft_group.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::CreateLink { parent, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_inode(parent.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::RemoveLink { parent, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_inode(parent.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::ReplaceLink { parent, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_inode(parent.into())
                .propose_archived(request)
                .await
        }
        ArchivedRkyvRequest::Hardlink {
            inode,
            new_parent,
            new_name,
            context,
        } => {
            hardlink_transaction(
                inode.into(),
                new_parent.into(),
                new_name.as_str(),
                context.into(),
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        ArchivedRkyvRequest::Rename {
            parent,
            name,
            new_parent,
            new_name,
            context,
        } => {
            rename_transaction(
                parent.into(),
                name.as_str(),
                new_parent.into(),
                new_name.as_str(),
                context.into(),
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        ArchivedRkyvRequest::Fsync { inode } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
        }
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
        ArchivedRkyvRequest::Lock { inode }
        | ArchivedRkyvRequest::Unlock { inode, .. }
        | ArchivedRkyvRequest::HardlinkRollback { inode, .. } => {
            raft.lookup_by_inode(inode.into())
                .propose_archived(request)
                .await
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
