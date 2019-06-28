use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::storage::raft_manager::RaftManager;
use crate::utils::{empty_response, finalize_response, FutureResultResponse};
use flatbuffers::FlatBufferBuilder;
use futures::future::{ok, result};
use futures::Future;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::sync::Arc;

// Sync to ensure replicas serve latest data
fn sync_with_leader(raft: &Arc<RaftManager>) -> impl Future<Item = (), Error = ErrorCode> {
    let cloned_raft = raft.clone();
    raft.get_latest_commit_from_leader()
        .map(move |latest_commit| cloned_raft.sync(latest_commit))
        .flatten()
        .map_err(|_| ErrorCode::Uncategorized)
}

pub fn request_router(
    request: GenericRequest,
    raft: Arc<RaftManager>,
    mut builder: FlatBufferBuilder<'static>,
) -> impl Future<Item = FlatBufferBuilder<'static>, Error = ErrorCode> {
    let response: Box<FutureResultResponse<'static>>;

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            let after_sync = sync_with_leader(&raft);
            let response_after_sync = after_sync
                .map(move |_| fsck(raft.local_context(), builder))
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::FilesystemChecksumRequest => {
            response = Box::new(result(checksum_request(raft.local_context(), builder)));
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            let after_sync = sync_with_leader(&raft);
            let inode = read_request.inode();
            let offset = read_request.offset();
            let read_size = read_request.read_size();
            let user_context = *read_request.context();
            let response_after_sync = after_sync
                .map(move |_| {
                    raft.file_storage()
                        .read(inode, offset, read_size, user_context, builder)
                })
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::ReadRawRequest => {
            let read_request = request.request_as_read_raw_request().unwrap();
            response = Box::new(result(raft.file_storage().read_raw(
                read_request.inode(),
                read_request.offset(),
                read_request.read_size(),
                builder,
            )));
        }
        RequestType::HardlinkRequest => unreachable!(),
        RequestType::RenameRequest => unreachable!(),
        RequestType::ChmodRequest => unreachable!(),
        RequestType::ChownRequest => unreachable!(),
        RequestType::TruncateRequest => unreachable!(),
        RequestType::FsyncRequest => unreachable!(),
        RequestType::CreateRequest => unreachable!(),
        RequestType::LookupRequest => {
            let lookup_request = request.request_as_lookup_request().unwrap();
            let after_sync = sync_with_leader(&raft);
            let parent = lookup_request.parent();
            let name = lookup_request.name().to_string();
            let user_context = *lookup_request.context();
            let response_after_sync = after_sync
                .map(move |_| {
                    raft.file_storage()
                        .lookup(parent, &name, user_context, builder)
                })
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::GetXattrRequest => {
            let get_xattr_request = request.request_as_get_xattr_request().unwrap();
            let after_sync = sync_with_leader(&raft);
            let inode = get_xattr_request.inode();
            let key = get_xattr_request.key().to_string();
            let response_after_sync = after_sync
                .map(move |_| raft.file_storage().get_xattr(inode, &key, builder))
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::ListXattrsRequest => {
            let list_xattrs_request = request.request_as_list_xattrs_request().unwrap();
            let after_sync = sync_with_leader(&raft);
            let inode = list_xattrs_request.inode();
            let response_after_sync = after_sync
                .map(move |_| raft.file_storage().list_xattrs(inode, builder))
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::SetXattrRequest => unreachable!(),
        RequestType::RemoveXattrRequest => unreachable!(),
        RequestType::UnlinkRequest => unreachable!(),
        RequestType::RmdirRequest => unreachable!(),
        RequestType::WriteRequest => unreachable!(),
        RequestType::UtimensRequest => unreachable!(),
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let after_sync = sync_with_leader(&raft);
            let inode = readdir_request.inode();
            let response_after_sync = after_sync
                .map(move |_| raft.file_storage().readdir(inode, builder))
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let after_sync = sync_with_leader(&raft);
            let inode = getattr_request.inode();
            let response_after_sync = after_sync
                .map(move |_| raft.file_storage().getattr(inode, builder))
                .flatten();
            response = Box::new(response_after_sync);
        }
        RequestType::MkdirRequest => unreachable!(),
        RequestType::RaftRequest => {
            let raft_request = request.request_as_raft_request().unwrap();
            let mut deserialized_message = Message::new();
            deserialized_message
                .merge_from_bytes(raft_request.message())
                .unwrap();
            raft.apply_messages(&[deserialized_message]).unwrap();
            response = Box::new(result(empty_response(builder)));
        }
        RequestType::LatestCommitRequest => {
            let index = raft.get_latest_local_commit();
            let mut response_builder = LatestCommitResponseBuilder::new(&mut builder);
            response_builder.add_index(index);
            let response_offset = response_builder.finish().as_union_value();
            response = Box::new(ok((
                builder,
                ResponseType::LatestCommitResponse,
                response_offset,
            )));
        }
        RequestType::GetLeaderRequest => {
            let leader_future = raft
                .get_leader()
                .map(move |leader_id| {
                    let mut response_builder = NodeIdResponseBuilder::new(&mut builder);
                    response_builder.add_node_id(leader_id);
                    let response_offset = response_builder.finish().as_union_value();
                    (builder, ResponseType::NodeIdResponse, response_offset)
                })
                .map_err(|_| ErrorCode::Uncategorized);

            response = Box::new(leader_future);
        }
        RequestType::NONE => unreachable!(),
    }

    response.map(|(mut builder, response_type, response_offset)| {
        finalize_response(&mut builder, response_type, response_offset);
        builder
    })
}
