use crate::file_handler::FileRequestHandler;
use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::raft_manager::RaftManager;
use crate::utils::{finalize_response, into_error_code, to_read_response, to_xattrs_response};
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures::future::result;
use futures::Future;
use std::sync::Arc;

pub fn request_router<'a, 'b>(
    request: GenericRequest<'a>,
    raft: Arc<RaftManager>,
    builder: FlatBufferBuilder<'b>,
) -> impl Future<Item = FlatBufferBuilder<'b>, Error = ErrorCode> {
    let response: Box<
        Future<
                Item = (
                    FlatBufferBuilder<'b>,
                    ResponseType,
                    WIPOffset<UnionWIPOffset>,
                ),
                Error = ErrorCode,
            > + Send,
    >;
    let data_storage = raft.data_storage();
    let metadata_storage = raft.metadata_storage();
    let context = raft.local_context();

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            response = Box::new(fsck(context, builder));
        }
        RequestType::FilesystemChecksumRequest => {
            response = Box::new(result(checksum_request(context, builder)));
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            let read_result = data_storage.read(
                read_request.path().trim_start_matches('/'),
                read_request.offset(),
                read_request.read_size(),
            );
            response =
                Box::new(read_result.map(move |data| to_read_response(builder, &data).unwrap()))
        }
        RequestType::ReadRawRequest => {
            let read_request = request.request_as_read_raw_request().unwrap();
            let read_result = data_storage.read_raw(
                read_request.path().trim_start_matches('/'),
                read_request.offset(),
                read_request.read_size(),
            );
            response = Box::new(result(
                read_result
                    .map(move |data| to_read_response(builder, &data).unwrap())
                    .map_err(into_error_code),
            ));
        }
        RequestType::HardlinkRequest => unreachable!(),
        RequestType::AccessRequest => {
            let access_request = request.request_as_access_request().unwrap();
            let file = FileRequestHandler::new(
                access_request.path().trim_start_matches('/').to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.access(
                access_request.uid(),
                &access_request.gids(),
                access_request.mask(),
                metadata_storage,
            )));
        }
        RequestType::RenameRequest => unreachable!(),
        RequestType::ChmodRequest => unreachable!(),
        RequestType::ChownRequest => unreachable!(),
        RequestType::TruncateRequest => unreachable!(),
        RequestType::FsyncRequest => unreachable!(),
        RequestType::GetXattrRequest => {
            let get_xattr_request = request.request_as_get_xattr_request().unwrap();
            // TODO: handle key doesn't exist
            let data = metadata_storage
                .get_xattr(
                    get_xattr_request.path().trim_start_matches('/'),
                    get_xattr_request.key(),
                )
                .unwrap_or_else(|| vec![]);
            response = Box::new(result(to_read_response(builder, &data)));
        }
        RequestType::ListXattrsRequest => {
            let list_xattrs_request = request.request_as_list_xattrs_request().unwrap();
            // TODO: handle key doesn't exist
            let attrs =
                metadata_storage.list_xattrs(list_xattrs_request.path().trim_start_matches('/'));
            response = Box::new(result(to_xattrs_response(builder, &attrs)));
        }
        RequestType::SetXattrRequest => unreachable!(),
        RequestType::RemoveXattrRequest => unreachable!(),
        RequestType::UnlinkRequest => unreachable!(),
        RequestType::RmdirRequest => unreachable!(),
        RequestType::WriteRequest => unreachable!(),
        RequestType::UtimensRequest => unreachable!(),
        RequestType::ReaddirRequest => {
            let readdir_request = request.request_as_readdir_request().unwrap();
            let file = FileRequestHandler::new(
                readdir_request.path().trim_start_matches('/').to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.readdir()));
        }
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            let file = FileRequestHandler::new(
                getattr_request.path().trim_start_matches('/').to_string(),
                context.data_dir.clone(),
                builder,
            );
            response = Box::new(result(file.getattr(metadata_storage)));
        }
        RequestType::MkdirRequest => unreachable!(),
        RequestType::RaftRequest => unreachable!(),
        RequestType::LatestCommitRequest => unreachable!(),
        RequestType::GetLeaderRequest => unreachable!(),
        RequestType::NONE => unreachable!(),
    }

    response.map(|(mut builder, response_type, response_offset)| {
        finalize_response(&mut builder, response_type, response_offset);
        builder
    })
}
