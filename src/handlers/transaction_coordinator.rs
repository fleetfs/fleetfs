use crate::generated::*;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::utils::{
    finalize_request_without_prefix, finalize_response, finalize_response_without_prefix,
    response_or_error, FlatBufferWithResponse,
};
use flatbuffers::{FlatBufferBuilder, SIZE_UOFFSET};
use std::sync::Arc;

fn hardlink_increment_request<'a>(
    inode: u64,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = HardlinkIncrementRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(
        builder,
        RequestType::HardlinkIncrementRequest,
        finish_offset,
    );

    get_root_as_generic_request(builder.finished_data())
}

fn hardlink_rollback_request<'a>(
    inode: u64,
    last_modified: Timestamp,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = HardlinkRollbackRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    request_builder.add_last_modified_time(&last_modified);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::HardlinkRollbackRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

fn create_inode_request<'a>(
    parent: Option<u64>,
    uid: u32,
    gid: u32,
    mode: u16,
    kind: FileKind,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = CreateInodeRequestBuilder::new(builder);
    request_builder.add_uid(uid);
    request_builder.add_gid(gid);
    request_builder.add_mode(mode);
    request_builder.add_kind(kind);
    request_builder.add_parent(parent.unwrap_or(0));
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::CreateInodeRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

fn create_link_request<'a>(
    parent: u64,
    name: &'_ str,
    inode: u64,
    kind: FileKind,
    context: UserContext,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let builder_name = builder.create_string(name);
    let mut request_builder = CreateLinkRequestBuilder::new(builder);
    request_builder.add_parent(parent);
    request_builder.add_name(builder_name);
    request_builder.add_inode(inode);
    request_builder.add_kind(kind);
    request_builder.add_context(&context);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::CreateLinkRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

fn decrement_inode_request<'a>(
    inode: u64,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = DecrementInodeRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::DecrementInodeRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn create_transaction<'a>(
    create_request: CreateRequest<'a>,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    let mut create_request_builder = FlatBufferBuilder::new();
    // First create inode. This effectively begins the transaction.
    let create_inode = create_inode_request(
        None,
        create_request.uid(),
        create_request.gid(),
        create_request.mode(),
        create_request.kind(),
        &mut create_request_builder,
    );

    // This will be the response back to the client, so use builder object
    let (mut builder, response_type, response_offset) = raft
        .least_loaded_group()
        .propose(create_inode, builder)
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let created_inode_response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(
        created_inode_response.response_type(),
        ResponseType::FileMetadataResponse
    );

    let inode = created_inode_response
        .response_as_file_metadata_response()
        .unwrap()
        .inode();

    // Second create the link
    let mut link_request_builder = FlatBufferBuilder::new();
    let create_link = create_link_request(
        create_request.parent(),
        create_request.name(),
        inode,
        create_request.kind(),
        UserContext::new(create_request.uid(), create_request.gid()),
        &mut link_request_builder,
    );

    match raft
        .lookup_by_inode(create_request.parent())
        .propose(create_link, FlatBufferBuilder::new())
        .await
    {
        Ok((mut response_buffer, response_type, response_offset)) => {
            finalize_response_without_prefix(&mut response_buffer, response_type, response_offset);
            if response_or_error(response_buffer.finished_data()).is_err() {
                // Rollback the transaction
                let mut internal_request_builder = FlatBufferBuilder::new();
                let rollback = decrement_inode_request(inode, &mut internal_request_builder);
                // TODO: if this fails the inode will leak ;(
                raft.lookup_by_inode(inode)
                    .propose(rollback, FlatBufferBuilder::new())
                    .await?;
            }

            // This is the response back to the client
            return Ok(FlatBufferWithResponse::new(builder));
        }
        Err(error_code) => {
            // Rollback the transaction
            let mut internal_request_builder = FlatBufferBuilder::new();
            let rollback = decrement_inode_request(inode, &mut internal_request_builder);
            // TODO: if this fails the inode will leak ;(
            raft.lookup_by_inode(inode)
                .propose(rollback, FlatBufferBuilder::new())
                .await?;
            return Err(error_code);
        }
    }
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn hardlink_transaction<'a, 'b>(
    hardlink_request: HardlinkRequest<'a>,
    mut builder: FlatBufferBuilder<'b>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'b>, ErrorCode> {
    let mut internal_request_builder = FlatBufferBuilder::new();

    // First increment the link count on the inode to ensure it can't be deleted
    // this effectively begins the transaction.
    let increment =
        hardlink_increment_request(hardlink_request.inode(), &mut internal_request_builder);

    let (mut response_buffer, response_type, response_offset) = raft
        .lookup_by_inode(hardlink_request.inode())
        .propose(increment, FlatBufferBuilder::new())
        .await?;
    finalize_response_without_prefix(&mut response_buffer, response_type, response_offset);
    let increment_response = response_or_error(response_buffer.finished_data())?;
    assert_eq!(
        increment_response.response_type(),
        ResponseType::HardlinkTransactionResponse
    );
    let transaction_response = increment_response
        .response_as_hardlink_transaction_response()
        .unwrap();
    let last_modified = *transaction_response.last_modified_time();
    let inode_kind = transaction_response.kind();

    // Second create the new link
    let mut internal_request_builder = FlatBufferBuilder::new();
    let create_link = create_link_request(
        hardlink_request.new_parent(),
        hardlink_request.new_name(),
        hardlink_request.inode(),
        inode_kind,
        *hardlink_request.context(),
        &mut internal_request_builder,
    );

    match raft
        .lookup_by_inode(hardlink_request.new_parent())
        .propose(create_link, FlatBufferBuilder::new())
        .await
    {
        Ok((mut response_buffer, response_type, response_offset)) => {
            finalize_response_without_prefix(&mut response_buffer, response_type, response_offset);
            if response_or_error(response_buffer.finished_data()).is_err() {
                // Rollback the transaction
                let mut internal_request_builder = FlatBufferBuilder::new();
                let rollback = hardlink_rollback_request(
                    hardlink_request.inode(),
                    last_modified,
                    &mut internal_request_builder,
                );
                // TODO: if this fails the filesystem is corrupted ;( since the link count
                // may not have been decremented
                raft.lookup_by_inode(hardlink_request.inode())
                    .propose(rollback, FlatBufferBuilder::new())
                    .await?;
            }

            // This is the response back to the client
            let mut response_builder = FileMetadataResponseBuilder::new(&mut builder);
            let attrs = transaction_response.attr_response();
            response_builder.add_inode(attrs.inode());
            response_builder.add_size_bytes(attrs.size_bytes());
            response_builder.add_size_blocks(attrs.size_blocks());
            response_builder.add_last_access_time(attrs.last_access_time());
            response_builder.add_last_modified_time(attrs.last_modified_time());
            response_builder.add_last_metadata_modified_time(attrs.last_metadata_modified_time());
            response_builder.add_kind(attrs.kind());
            response_builder.add_mode(attrs.mode());
            response_builder.add_hard_links(attrs.hard_links());
            response_builder.add_user_id(attrs.user_id());
            response_builder.add_group_id(attrs.group_id());
            response_builder.add_device_id(attrs.device_id());

            let offset = response_builder.finish().as_union_value();
            finalize_response(&mut builder, ResponseType::FileMetadataResponse, offset);
            return Ok(FlatBufferWithResponse::new(builder));
        }
        Err(error_code) => {
            // Rollback the transaction
            let mut internal_request_builder = FlatBufferBuilder::new();
            let rollback = hardlink_rollback_request(
                hardlink_request.inode(),
                last_modified,
                &mut internal_request_builder,
            );
            // TODO: if this fails the filesystem is corrupted ;( since the link count
            // may not have been decremented
            raft.lookup_by_inode(hardlink_request.inode())
                .propose(rollback, FlatBufferBuilder::new())
                .await?;
            return Err(error_code);
        }
    }
}
