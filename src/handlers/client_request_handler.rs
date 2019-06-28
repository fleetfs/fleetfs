use crate::generated::*;
use crate::handlers::router::request_router;
use crate::storage::raft_manager::RaftManager;
use crate::utils::{finalize_response, is_write_request};
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
    // TODO: merge these branches, so that request_router handles all of them
    let builder_future: Box<Future<Item = FlatBufferBuilder, Error = ErrorCode> + Send> =
        if is_write_request(request.request_type()) {
            Box::new(
                raft.propose(request, builder)
                    .map_err(|_| ErrorCode::Uncategorized),
            )
        } else {
            Box::new(request_router(request, raft, builder))
        };
    builder_future.or_else(|error_code| {
        let mut builder = FlatBufferBuilder::new();
        let args = ErrorResponseArgs { error_code };
        let response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
        finalize_response(&mut builder, ResponseType::ErrorResponse, response_offset);

        Ok(builder)
    })
}
