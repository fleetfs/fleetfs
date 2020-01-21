use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::handlers::router::FullOrPartialResponse::{Full, Partial};
use crate::storage::raft_manager::RaftManager;
use crate::utils::{empty_response, finalize_response, FlatBufferResponse, FlatBufferWithResponse};
use flatbuffers::FlatBufferBuilder;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::sync::Arc;

// Sync to ensure replicas serve latest data
async fn sync_with_leader(raft: Arc<RaftManager>) -> Result<(), ErrorCode> {
    let latest_commit = raft.get_latest_commit_from_leader().await?;
    raft.sync(latest_commit).await
}

enum FullOrPartialResponse {
    Full(FlatBufferWithResponse<'static>),
    Partial(FlatBufferResponse<'static>),
}

async fn request_router_inner(
    request: GenericRequest<'_>,
    raft: Arc<RaftManager>,
    mut builder: FlatBufferBuilder<'static>,
) -> Result<FullOrPartialResponse, ErrorCode> {
    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            sync_with_leader(raft.clone()).await?;
            return fsck(raft.local_context().clone(), builder)
                .await
                .map(Partial);
        }
        RequestType::FilesystemChecksumRequest => {
            return checksum_request(raft.local_context(), builder).map(Partial);
        }
        RequestType::ReadRequest => {
            if let Some(read_request) = request.request_as_read_request() {
                sync_with_leader(raft.clone()).await?;
                let inode = read_request.inode();
                let offset = read_request.offset();
                let read_size = read_request.read_size();
                return raft
                    .file_storage()
                    .read(inode, offset, read_size, builder)
                    .await
                    .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReadRawRequest => {
            if let Some(read_request) = request.request_as_read_raw_request() {
                return Ok(Full(raft.file_storage().read_raw(
                    read_request.inode(),
                    read_request.offset(),
                    read_request.read_size(),
                    builder,
                )));
            } else {
                return Err(ErrorCode::BadRequest);
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
            return raft.propose(request, builder).await.map(Partial);
        }
        RequestType::LookupRequest => {
            if let Some(lookup_request) = request.request_as_lookup_request() {
                sync_with_leader(raft.clone()).await?;
                let parent = lookup_request.parent();
                let name = lookup_request.name().to_string();
                let user_context = *lookup_request.context();
                return raft
                    .file_storage()
                    .lookup(parent, &name, user_context, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetXattrRequest => {
            if let Some(get_xattr_request) = request.request_as_get_xattr_request() {
                sync_with_leader(raft.clone()).await?;
                let inode = get_xattr_request.inode();
                let key = get_xattr_request.key().to_string();
                return raft
                    .file_storage()
                    .get_xattr(inode, &key, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ListXattrsRequest => {
            if let Some(list_xattrs_request) = request.request_as_list_xattrs_request() {
                sync_with_leader(raft.clone()).await?;
                let inode = list_xattrs_request.inode();
                return raft.file_storage().list_xattrs(inode, builder).map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReaddirRequest => {
            if let Some(readdir_request) = request.request_as_readdir_request() {
                sync_with_leader(raft.clone()).await?;
                let inode = readdir_request.inode();
                return raft.file_storage().readdir(inode, builder).map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetattrRequest => {
            if let Some(getattr_request) = request.request_as_getattr_request() {
                sync_with_leader(raft.clone()).await?;
                let inode = getattr_request.inode();
                return raft.file_storage().getattr(inode, builder).map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RaftRequest => {
            if let Some(raft_request) = request.request_as_raft_request() {
                let mut deserialized_message = Message::new();
                deserialized_message
                    .merge_from_bytes(raft_request.message())
                    .unwrap();
                raft.apply_messages(&[deserialized_message]).unwrap();
                return empty_response(builder).map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LatestCommitRequest => {
            let index = raft.get_latest_local_commit();
            let mut response_builder = LatestCommitResponseBuilder::new(&mut builder);
            response_builder.add_index(index);
            let response_offset = response_builder.finish().as_union_value();
            return Ok(Partial((
                builder,
                ResponseType::LatestCommitResponse,
                response_offset,
            )));
        }
        RequestType::FilesystemReadyRequest => {
            raft.get_leader().await?;
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            let response_offset = response_builder.finish().as_union_value();
            return Ok(Partial((
                builder,
                ResponseType::EmptyResponse,
                response_offset,
            )));
        }
        RequestType::NONE => unreachable!(),
    }
}

pub async fn request_router<'a>(
    request: GenericRequest<'a>,
    raft: Arc<RaftManager>,
    builder: FlatBufferBuilder<'static>,
) -> FlatBufferWithResponse<'static> {
    match request_router_inner(request, raft, builder).await {
        Ok(response) => match response {
            Full(full_response) => return full_response,
            Partial((mut builder, response_type, response_offset)) => {
                finalize_response(&mut builder, response_type, response_offset);
                return FlatBufferWithResponse::new(builder);
            }
        },
        Err(error_code) => {
            let mut builder = FlatBufferBuilder::new();
            let args = ErrorResponseArgs { error_code };
            let response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
            finalize_response(&mut builder, ResponseType::ErrorResponse, response_offset);
            return FlatBufferWithResponse::new(builder);
        }
    };
}
