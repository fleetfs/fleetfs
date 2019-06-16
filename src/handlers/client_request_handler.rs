use crate::file_handler::file_request_handler;
use crate::generated::*;
use crate::handlers::raft_handler::raft_handler;
use crate::raft_manager::RaftManager;
use crate::utils::{finalize_response, is_raft_request, is_write_request};
use bytes::BytesMut;
use flatbuffers::FlatBufferBuilder;
use futures::Future;
use std::sync::Arc;

// Routes requests from client to the appropriate handler
pub fn client_request_handler(
    mut builder: FlatBufferBuilder<'static>,
    frame: BytesMut,
    raft: Arc<RaftManager>,
) -> impl Future<Item = FlatBufferBuilder<'static>, Error = std::io::Error> {
    let request = get_root_as_generic_request(&frame);
    builder.reset();
    let builder_future: Box<Future<Item = FlatBufferBuilder, Error = ErrorCode> + Send>;
    if is_raft_request(request.request_type()) {
        builder_future = Box::new(raft_handler(request, &raft, builder));
    } else if is_write_request(request.request_type()) {
        builder_future = Box::new(
            raft.propose(request, builder)
                .map_err(|_| ErrorCode::Uncategorized),
        );
    } else {
        // Sync to ensure replicas serve latest data
        let cloned_raft = raft.clone();
        let after_sync = raft
            .get_latest_commit_from_leader()
            .map(move |latest_commit| cloned_raft.sync(latest_commit))
            .flatten()
            .map_err(|_| ErrorCode::Uncategorized);
        let cloned_raft = raft.clone();
        let read_after_sync = after_sync
            .map(move |_| {
                let request = get_root_as_generic_request(&frame);
                file_request_handler(
                    request,
                    cloned_raft.data_storage(),
                    cloned_raft.metadata_storage(),
                    &cloned_raft.local_context(),
                    builder,
                )
            })
            .flatten();
        builder_future = Box::new(read_after_sync);
    }
    Box::new(builder_future.or_else(|error_code| {
        let mut builder = FlatBufferBuilder::new();
        let args = ErrorResponseArgs { error_code };
        let response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
        finalize_response(&mut builder, ResponseType::ErrorResponse, response_offset);

        Ok(builder)
    }))
}
