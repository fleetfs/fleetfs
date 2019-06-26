use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::storage::raft_manager::RaftManager;
use crate::utils::finalize_response;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures::future::result;
use futures::Future;
use std::sync::Arc;

pub fn request_router(
    request: GenericRequest,
    raft: Arc<RaftManager>,
    builder: FlatBufferBuilder<'static>,
) -> impl Future<Item = FlatBufferBuilder<'static>, Error = ErrorCode> {
    let response: Box<
        Future<
                Item = (
                    FlatBufferBuilder<'static>,
                    ResponseType,
                    WIPOffset<UnionWIPOffset>,
                ),
                Error = ErrorCode,
            > + Send,
    >;
    let context = raft.local_context();
    let file = raft.file_storage();

    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            response = Box::new(fsck(context, builder));
        }
        RequestType::FilesystemChecksumRequest => {
            response = Box::new(result(checksum_request(context, builder)));
        }
        RequestType::ReadRequest => {
            let read_request = request.request_as_read_request().unwrap();
            response = Box::new(file.read(
                read_request.inode(),
                read_request.offset(),
                read_request.read_size(),
                *read_request.context(),
                builder,
            ));
        }
        RequestType::ReadRawRequest => {
            let read_request = request.request_as_read_raw_request().unwrap();
            response = Box::new(result(file.read_raw(
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
            response = Box::new(result(file.lookup(
                lookup_request.parent(),
                lookup_request.name(),
                lookup_request.uid(),
                lookup_request.gid(),
                builder,
            )));
        }
        RequestType::GetXattrRequest => {
            let get_xattr_request = request.request_as_get_xattr_request().unwrap();
            response = Box::new(result(file.get_xattr(
                get_xattr_request.inode(),
                get_xattr_request.key(),
                builder,
            )));
        }
        RequestType::ListXattrsRequest => {
            let list_xattrs_request = request.request_as_list_xattrs_request().unwrap();
            response = Box::new(result(
                file.list_xattrs(list_xattrs_request.inode(), builder),
            ));
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
