use std::cell::{RefCell, RefMut};
use std::ffi::OsString;
use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;
use fuse_mt::{DirectoryEntry, FileAttr};
use thread_local::CachedThreadLocal;
use time::Timespec;

use crate::generated::*;
use crate::tcp_client::TcpClient;
use crate::utils::{finalize_request, response_or_error};

fn file_type_to_fuse_type(file_type: FileKind) -> fuse_mt::FileType {
    match file_type {
        FileKind::File => fuse_mt::FileType::RegularFile,
        FileKind::Directory => fuse_mt::FileType::Directory,
        FileKind::DefaultValueNotAType => unreachable!(),
    }
}

fn metadata_to_fuse_fileattr(metadata: &FileMetadataResponse) -> FileAttr {
    FileAttr {
        size: metadata.size_bytes(),
        blocks: metadata.size_blocks(),
        atime: Timespec {
            sec: metadata.last_access_time().seconds(),
            nsec: metadata.last_access_time().nanos(),
        },
        mtime: Timespec {
            sec: metadata.last_modified_time().seconds(),
            nsec: metadata.last_modified_time().nanos(),
        },
        ctime: Timespec {
            sec: metadata.last_metadata_modified_time().seconds(),
            nsec: metadata.last_metadata_modified_time().nanos(),
        },
        crtime: Timespec { sec: 0, nsec: 0 },
        kind: file_type_to_fuse_type(metadata.kind()),
        perm: metadata.mode(),
        nlink: metadata.hard_links(),
        uid: metadata.user_id(),
        gid: metadata.group_id(),
        rdev: metadata.device_id(),
        flags: 0,
    }
}

pub struct NodeClient<'a> {
    tcp_client: TcpClient,
    response_buffer: CachedThreadLocal<RefCell<Vec<u8>>>,
    request_builder: CachedThreadLocal<RefCell<FlatBufferBuilder<'a>>>,
}

impl<'a> NodeClient<'a> {
    pub fn new(server_ip_port: SocketAddr) -> NodeClient<'a> {
        NodeClient {
            tcp_client: TcpClient::new(server_ip_port),
            response_buffer: CachedThreadLocal::new(),
            request_builder: CachedThreadLocal::new(),
        }
    }

    fn get_or_create_builder(&self) -> RefMut<FlatBufferBuilder<'a>> {
        let mut builder = self
            .request_builder
            .get_or(|| Box::new(RefCell::new(FlatBufferBuilder::new())))
            .borrow_mut();
        builder.reset();
        return builder;
    }

    fn get_or_create_buffer(&self) -> RefMut<Vec<u8>> {
        return self
            .response_buffer
            .get_or(|| Box::new(RefCell::new(vec![])))
            .borrow_mut();
    }

    fn send<'b>(
        &self,
        request: &[u8],
        buffer: &'b mut Vec<u8>,
    ) -> Result<GenericResponse<'b>, ErrorCode> {
        self.tcp_client
            .send_and_receive_length_prefixed(request, buffer.as_mut())
            .map_err(|_| ErrorCode::Uncategorized)?;
        return response_or_error(buffer);
    }

    // TODO move into an internal peer client
    pub fn filesystem_checksum(&self) -> Result<Vec<u8>, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let request_builder = FilesystemChecksumRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::FilesystemChecksumRequest,
            finish_offset,
        );

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let read_response = response.response_as_read_response().unwrap();

        return Ok(read_response.data().to_vec());
    }

    pub fn fsck(&self) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let request_builder = FilesystemCheckRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::FilesystemCheckRequest,
            finish_offset,
        );

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn mkdir(&self, path: &str, mode: u16) -> Result<FileAttr, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = MkdirRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_mode(mode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::MkdirRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response.response_as_file_metadata_response().unwrap();

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn getattr(&self, path: &str) -> Result<FileAttr, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = GetattrRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::GetattrRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response.response_as_file_metadata_response().unwrap();

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn utimens(
        &self,
        path: &str,
        atime_secs: i64,
        atime_nanos: i32,
        mtime_secs: i64,
        mtime_nanos: i32,
    ) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = UtimensRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let atime = Timestamp::new(atime_secs, atime_nanos);
        request_builder.add_atime(&atime);
        let mtime = Timestamp::new(mtime_secs, mtime_nanos);
        request_builder.add_mtime(&mtime);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::UtimensRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn chmod(&self, path: &str, mode: u32) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = ChmodRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_mode(mode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ChmodRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn hardlink(&self, path: &str, new_path: &str) -> Result<FileAttr, ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let builder_new_path = builder.create_string(new_path);
        let mut request_builder = HardlinkRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_path(builder_new_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::HardlinkRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response.response_as_file_metadata_response().unwrap();

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn rename(&self, path: &str, new_path: &str) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let builder_new_path = builder.create_string(new_path);
        let mut request_builder = RenameRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_path(builder_new_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RenameRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn read<F: FnOnce(Result<&[u8], ErrorCode>) -> ()>(
        &self,
        path: &str,
        offset: u64,
        size: u32,
        callback: F,
    ) {
        assert_ne!(path, "/");

        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = match self.send(builder.finished_data(), &mut buffer) {
            Ok(response) => response,
            Err(e) => {
                callback(Err(e));
                return;
            }
        };
        let data = response.response_as_read_response().unwrap().data();

        callback(Ok(data));
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<DirectoryEntry>, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = ReaddirRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReaddirRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;

        let mut result = vec![];
        let listing_response = response.response_as_directory_listing_response().unwrap();
        let entries = listing_response.entries();
        for i in 0..entries.len() {
            let entry = entries.get(i);
            result.push(DirectoryEntry {
                name: OsString::from(entry.path()),
                kind: file_type_to_fuse_type(entry.kind()),
            });
        }

        return Ok(result);
    }

    pub fn truncate(&self, path: &str, length: u64) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = TruncateRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_length(length);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::TruncateRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn write(&self, path: &str, data: &[u8], offset: u64) -> Result<u32, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let data_offset = builder.create_vector_direct(data);
        let mut request_builder = WriteRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_offset(offset);
        request_builder.add_data(data_offset);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::WriteRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        return Ok(response
            .response_as_written_response()
            .unwrap()
            .bytes_written());
    }

    pub fn unlink(&self, path: &str) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_path = builder.create_string(path);
        let mut request_builder = UnlinkRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::UnlinkRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }
}
