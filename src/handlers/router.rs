use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::storage::raft_manager::RaftManager;
use crate::utils::{empty_response, finalize_response, FlatBufferWithResponse, ResultResponse};
use flatbuffers::FlatBufferBuilder;
use futures::FutureExt;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::sync::Arc;

// Sync to ensure replicas serve latest data
async fn sync_with_leader(raft: Arc<RaftManager>) -> Result<(), ErrorCode> {
    let latest_commit = raft.get_latest_commit_from_leader().await?;
    raft.sync(latest_commit).await
}

pub async fn request_router<'a>(
    request: GenericRequest<'a>,
    raft: Arc<RaftManager>,
    mut builder: FlatBufferBuilder<'static>,
) -> FlatBufferWithResponse<'static> {
    let response: ResultResponse<'static>;

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            let after_sync = sync_with_leader(raft.clone());
            let response_after_sync =
                after_sync.then(move |_| fsck(raft.local_context().clone(), builder));
            response = response_after_sync.await;
        }
        RequestType::FilesystemChecksumRequest => {
            response = checksum_request(raft.local_context(), builder);
        }
        RequestType::ReadRequest => {
            if let Some(read_request) = request.request_as_read_request() {
                let after_sync = sync_with_leader(raft.clone());
                let inode = read_request.inode();
                let offset = read_request.offset();
                let read_size = read_request.read_size();
                if let Err(error_code) = after_sync.await {
                    response = Err(error_code);
                } else {
                    match raft
                        .file_storage()
                        .read(inode, offset, read_size, builder)
                        .await
                    {
                        Ok(read_response) => return read_response,
                        Err(error_code) => response = Err(error_code),
                    }
                }
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReadRawRequest => {
            if let Some(read_request) = request.request_as_read_raw_request() {
                return raft.file_storage().read_raw(
                    read_request.inode(),
                    read_request.offset(),
                    read_request.read_size(),
                    builder,
                );
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::SetXattrRequest
        | RequestType::RemoveXattrRequest
        | RequestType::UnlinkRequest
        | RequestType::RmdirRequest
        | RequestType::WriteRequest
        | RequestType::UtimensRequest
        | RequestType::HardlinkRequest
        | RequestType::RenameRequest
        | RequestType::MkdirRequest
        | RequestType::ChmodRequest
        | RequestType::ChownRequest
        | RequestType::TruncateRequest
        | RequestType::FsyncRequest
        | RequestType::CreateRequest => {
            response = raft.propose(request, builder).await;
        }
        RequestType::LookupRequest => {
            if let Some(lookup_request) = request.request_as_lookup_request() {
                let after_sync = sync_with_leader(raft.clone());
                let parent = lookup_request.parent();
                let name = lookup_request.name().to_string();
                let user_context = *lookup_request.context();
                let response_after_sync = after_sync.map(move |_| {
                    raft.file_storage()
                        .lookup(parent, &name, user_context, builder)
                });
                response = response_after_sync.await;
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetXattrRequest => {
            if let Some(get_xattr_request) = request.request_as_get_xattr_request() {
                let after_sync = sync_with_leader(raft.clone());
                let inode = get_xattr_request.inode();
                let key = get_xattr_request.key().to_string();
                let response_after_sync =
                    after_sync.map(move |_| raft.file_storage().get_xattr(inode, &key, builder));
                response = response_after_sync.await;
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ListXattrsRequest => {
            if let Some(list_xattrs_request) = request.request_as_list_xattrs_request() {
                let after_sync = sync_with_leader(raft.clone());
                let inode = list_xattrs_request.inode();
                let response_after_sync =
                    after_sync.map(move |_| raft.file_storage().list_xattrs(inode, builder));
                response = response_after_sync.await;
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReaddirRequest => {
            if let Some(readdir_request) = request.request_as_readdir_request() {
                let after_sync = sync_with_leader(raft.clone());
                let inode = readdir_request.inode();
                let response_after_sync =
                    after_sync.map(move |_| raft.file_storage().readdir(inode, builder));
                response = response_after_sync.await;
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetattrRequest => {
            if let Some(getattr_request) = request.request_as_getattr_request() {
                let after_sync = sync_with_leader(raft.clone());
                let inode = getattr_request.inode();
                let response_after_sync =
                    after_sync.map(move |_| raft.file_storage().getattr(inode, builder));
                response = response_after_sync.await;
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RaftRequest => {
            if let Some(raft_request) = request.request_as_raft_request() {
                let mut deserialized_message = Message::new();
                deserialized_message
                    .merge_from_bytes(raft_request.message())
                    .unwrap();
                raft.apply_messages(&[deserialized_message]).unwrap();
                response = empty_response(builder);
            } else {
                response = Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LatestCommitRequest => {
            let index = raft.get_latest_local_commit();
            let mut response_builder = LatestCommitResponseBuilder::new(&mut builder);
            response_builder.add_index(index);
            let response_offset = response_builder.finish().as_union_value();
            response = Ok((builder, ResponseType::LatestCommitResponse, response_offset));
        }
        RequestType::GetLeaderRequest => {
            let leader_future = raft.get_leader().map(move |leader_id| {
                leader_id.map(|id| {
                    let mut response_builder = NodeIdResponseBuilder::new(&mut builder);
                    response_builder.add_node_id(id);
                    let response_offset = response_builder.finish().as_union_value();
                    (builder, ResponseType::NodeIdResponse, response_offset)
                })
            });

            response = leader_future.await;
        }
        RequestType::NONE => unreachable!(),
    }

    let builder = response
        .map(|(mut builder, response_type, response_offset)| {
            finalize_response(&mut builder, response_type, response_offset);
            builder
        })
        .unwrap_or_else(|error_code| {
            let mut builder = FlatBufferBuilder::new();
            let args = ErrorResponseArgs { error_code };
            let response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
            finalize_response(&mut builder, ResponseType::ErrorResponse, response_offset);

            builder
        });
    FlatBufferWithResponse::new(builder)
}
