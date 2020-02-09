use crate::generated::*;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::utils::{
    finalize_request_without_prefix, finalize_response, finalize_response_without_prefix,
    response_or_error, FlatBufferWithResponse,
};
use flatbuffers::FlatBufferBuilder;
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

fn hardlink_create_request<'a>(
    inode: u64,
    inode_kind: FileKind,
    new_parent: u64,
    new_name: &'_ str,
    context: UserContext,
    builder: &'a mut FlatBufferBuilder<'a>,
) -> GenericRequest<'a> {
    let builder_new_name = builder.create_string(new_name);
    let mut request_builder = HardlinkCreateRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    request_builder.add_inode_kind(inode_kind);
    request_builder.add_new_parent(new_parent);
    request_builder.add_new_name(builder_new_name);
    request_builder.add_context(&context);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::HardlinkCreateRequest, finish_offset);

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
    let create_link = hardlink_create_request(
        hardlink_request.inode(),
        inode_kind,
        hardlink_request.new_parent(),
        hardlink_request.new_name(),
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
