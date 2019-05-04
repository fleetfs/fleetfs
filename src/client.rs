use std::ffi::OsString;
use std::net::SocketAddr;

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use fuse_mt::{FileAttr, DirectoryEntry};
use time::Timespec;

use crate::tcp_client::TcpClient;
use crate::generated::*;

fn finalize_request(builder: &mut FlatBufferBuilder, request_type: RequestType, finish_offset: WIPOffset<UnionWIPOffset>) {
    let mut generic_request_builder = GenericRequestBuilder::new(builder);
    generic_request_builder.add_request_type(request_type);
    generic_request_builder.add_request(finish_offset);
    let finish_offset = generic_request_builder.finish();
    builder.finish_size_prefixed(finish_offset, None);
}

fn file_type_to_fuse_type(file_type: FileType) -> fuse_mt::FileType {
    match file_type {
        FileType::File => fuse_mt::FileType::RegularFile,
        FileType::Directory => fuse_mt::FileType::Directory,
        FileType::DefaultValueNotAType => unreachable!()
    }
}

fn metadata_to_fuse_fileattr(metadata: &FileMetadataResponse) -> FileAttr {
    FileAttr {
        size: metadata.size_bytes(),
        blocks: metadata.size_blocks(),
        atime: Timespec {sec: metadata.last_access_time().seconds(), nsec: metadata.last_access_time().nanos()},
        mtime: Timespec {sec: metadata.last_modified_time().seconds(), nsec: metadata.last_modified_time().nanos()},
        ctime: Timespec {sec: metadata.last_metadata_modified_time().seconds(), nsec: metadata.last_metadata_modified_time().nanos()},
        crtime: Timespec { sec: 0, nsec: 0 },
        kind: file_type_to_fuse_type(metadata.kind()),
        perm: metadata.mode(),
        nlink: metadata.hard_links(),
        uid: metadata.user_id(),
        gid: metadata.group_id(),
        rdev: metadata.device_id(),
        flags: 0
    }
}

fn response_or_error(buffer: &[u8]) -> Result<GenericResponse, ErrorCode> {
    let response = flatbuffers::get_root::<GenericResponse>(buffer);
    if response.response_type() == ResponseType::ErrorResponse {
        let error = response.response_as_error_response().unwrap();
        return Err(error.error_code());
    }
    return Ok(response);
}

pub struct NodeClient {
    tcp_client: TcpClient
}

impl NodeClient {
    pub fn new(server_ip_port: &SocketAddr) -> NodeClient {
        NodeClient {
            tcp_client: TcpClient::new(server_ip_port.clone())
        }
    }

    pub fn mkdir(&self, path: &String, mode: u16, forward: bool) -> Result<FileAttr, ErrorCode> {
        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = MkdirRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_mode(mode);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::MkdirRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        let metadata = response.response_as_file_metadata_response().unwrap();

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn getattr(&self, path: &String) -> Result<FileAttr, ErrorCode> {
        if path.len() == 1 {
            return Ok(FileAttr {
                size: 0,
                blocks: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                crtime: Timespec { sec: 0, nsec: 0 },
                kind: fuse_mt::FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0
            });
        }

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = GetattrRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::GetattrRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        let metadata = response.response_as_file_metadata_response().unwrap();

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn utimens(&self, path: &String, atime_secs: i64, atime_nanos: i32, mtime_secs: i64, mtime_nanos: i32, forward: bool) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = UtimensRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let atime = Timestamp::new(atime_secs, atime_nanos);
        request_builder.add_atime(&atime);
        let mtime = Timestamp::new(mtime_secs, mtime_nanos);
        request_builder.add_mtime(&mtime);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::UtimensRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn chmod(&self, path: &String, mode: u32, forward: bool) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ChmodRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_mode(mode);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ChmodRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn hardlink(&self, path: &String, new_path: &String, forward: bool) -> Result<FileAttr, ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let builder_new_path = builder.create_string(new_path.as_str());
        let mut request_builder = HardlinkRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_path(builder_new_path);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::HardlinkRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        let metadata = response.response_as_file_metadata_response().unwrap();

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn rename(&self, path: &String, new_path: &String, forward: bool) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let builder_new_path = builder.create_string(new_path.as_str());
        let mut request_builder = RenameRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_path(builder_new_path);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RenameRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn read(&self, path: &String, offset: u64, size: u32) -> Result<Vec<u8>, ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        let data = response.response_as_read_response().unwrap().data().to_vec();

        return Ok(data);
    }

    pub fn readdir(&self, path: &String) -> Result<Vec<DirectoryEntry>, ErrorCode> {
        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ReaddirRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReaddirRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;

        let mut result = vec![];
        let listing_response = response.response_as_directory_listing_response().unwrap();
        let entries = listing_response.entries();
        for i in 0..entries.len() {
            let entry = entries.get(i);
            result.push(DirectoryEntry {
                name: OsString::from(entry.path()),
                kind: file_type_to_fuse_type(entry.kind())
            });
        }

        return Ok(result);
    }

    pub fn truncate(&self, path: &String, length: u64, forward: bool) -> Result<(), ErrorCode> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = TruncateRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_length(length);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::TruncateRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn write(&self, path: &String, data: &[u8], offset: u64, forward: bool) -> Result<u32, ErrorCode> {
        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let data_offset = builder.create_vector_direct(data);
        let mut request_builder = WriteRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_offset(offset);
        request_builder.add_data(data_offset);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::WriteRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        return Ok(response.response_as_written_response().unwrap().bytes_written());
    }

    pub fn unlink(&self, path: &String, forward: bool) -> Result<(), ErrorCode> {
        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = UnlinkRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::UnlinkRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| ErrorCode::Uncategorized)?;
        let response = response_or_error(&buffer)?;
        response.response_as_empty_response().unwrap();

        return Ok(());
    }
}

