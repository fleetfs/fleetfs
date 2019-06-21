use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::storage::raft_manager::RaftManager;
use crate::utils::{
    finalize_response, into_error_code, to_inode_response, to_read_response, to_xattrs_response,
};
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures::future::{err, result};
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
    let context = raft.local_context();
    let file = raft.file_storage();
    let data_storage = file.get_data_storage();
    let metadata_storage = file.get_metadata_storage();

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            response = Box::new(fsck(context, builder));
        }
        RequestType::FilesystemChecksumRequest => {
            response = Box::new(result(checksum_request(context, builder)));
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            match metadata_storage.read(read_request.inode(), *read_request.context()) {
                Ok(_) => {
                    let read_result = data_storage.read(
                        read_request.inode(),
                        read_request.offset(),
                        read_request.read_size(),
                    );
                    response = Box::new(
                        read_result.map(move |data| to_read_response(builder, &data).unwrap()),
                    )
                }
                Err(error_code) => response = Box::new(err(error_code)),
            }
        }
        RequestType::ReadRawRequest => {
            let read_request = request.request_as_read_raw_request().unwrap();
            let read_result = data_storage.read_raw(
                read_request.inode(),
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
        RequestType::RenameRequest => unreachable!(),
        RequestType::ChmodRequest => unreachable!(),
        RequestType::ChownRequest => unreachable!(),
        RequestType::TruncateRequest => unreachable!(),
        RequestType::FsyncRequest => unreachable!(),
        RequestType::CreateRequest => unreachable!(),
        RequestType::LookupRequest => {
            let lookup_request = request.request_as_lookup_request().unwrap();
            // TODO: handle key doesn't exist
            match metadata_storage.lookup(
                lookup_request.parent(),
                lookup_request.name(),
                lookup_request.uid(),
                lookup_request.gid(),
            ) {
                Ok(maybe_inode) => {
                    if let Some(inode) = maybe_inode {
                        response = Box::new(result(to_inode_response(builder, inode)));
                    } else {
                        response = Box::new(err(ErrorCode::DoesNotExist));
                    }
                }
                Err(error_code) => response = Box::new(err(error_code)),
            }
        }
        RequestType::GetXattrRequest => {
            let get_xattr_request = request.request_as_get_xattr_request().unwrap();
            // TODO: handle key doesn't exist
            let data = metadata_storage
                .get_xattr(get_xattr_request.inode(), get_xattr_request.key())
                .unwrap_or_else(|| vec![]);
            response = Box::new(result(to_read_response(builder, &data)));
        }
        RequestType::ListXattrsRequest => {
            let list_xattrs_request = request.request_as_list_xattrs_request().unwrap();
            // TODO: handle key doesn't exist
            let attrs = metadata_storage.list_xattrs(list_xattrs_request.inode());
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
            response = Box::new(result(file.readdir(readdir_request.inode(), builder)));
        }
        RequestType::GetattrRequest => {
            let getattr_request = request.request_as_getattr_request().unwrap();
            response = Box::new(result(file.getattr(getattr_request.inode(), builder)));
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
