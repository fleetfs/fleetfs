use flatbuffers::EndianScalar;
use std::cell::{RefCell, RefMut};
use std::ffi::OsString;
use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;
use thread_local::ThreadLocal;

use crate::base::message_types::{ArchivedRkyvGenericResponse, RkyvGenericResponse};
use crate::base::{finalize_request, response_or_error};
use crate::client::tcp_client::TcpClient;
use crate::generated::*;
use crate::storage::ROOT_INODE;
use fuser::FileAttr;
use rkyv::AlignedVec;
use std::ops::Add;
use std::time::{Duration, SystemTime};

fn to_fuse_file_type(file_type: FileKind) -> fuser::FileType {
    match file_type {
        FileKind::File => fuser::FileType::RegularFile,
        FileKind::Directory => fuser::FileType::Directory,
        FileKind::Symlink => fuser::FileType::Symlink,
        FileKind::DefaultValueNotAType => unreachable!(),
    }
}

pub fn decode_fast_read_response_inplace(response: &mut Vec<u8>) -> Result<&Vec<u8>, ErrorCode> {
    let value = response.pop().unwrap().from_little_endian() as i8;
    let p = &value as *const i8 as *const ErrorCode;
    let error_code = unsafe { *p };
    if error_code == ErrorCode::DefaultValueNotAnError {
        Ok(response)
    } else {
        Err(error_code)
    }
}

pub struct StatFS {
    pub block_size: u32,
    pub max_name_length: u32,
}

fn metadata_to_fuse_fileattr(metadata: &FileMetadataResponse) -> FileAttr {
    FileAttr {
        ino: metadata.inode(),
        size: metadata.size_bytes(),
        blocks: metadata.size_blocks(),
        atime: SystemTime::UNIX_EPOCH.add(Duration::new(
            metadata.last_access_time().seconds() as u64,
            metadata.last_access_time().nanos() as u32,
        )),
        mtime: SystemTime::UNIX_EPOCH.add(Duration::new(
            metadata.last_modified_time().seconds() as u64,
            metadata.last_modified_time().nanos() as u32,
        )),
        ctime: SystemTime::UNIX_EPOCH.add(Duration::new(
            metadata.last_metadata_modified_time().seconds() as u64,
            metadata.last_metadata_modified_time().nanos() as u32,
        )),
        crtime: SystemTime::UNIX_EPOCH,
        kind: to_fuse_file_type(metadata.kind()),
        perm: metadata.mode(),
        nlink: metadata.hard_links(),
        uid: metadata.user_id(),
        gid: metadata.group_id(),
        rdev: metadata.device_id(),
        flags: 0,
        blksize: metadata.block_size(),
    }
}

pub struct NodeClient {
    tcp_client: TcpClient,
    response_buffer: ThreadLocal<RefCell<Vec<u8>>>,
    request_builder: ThreadLocal<RefCell<FlatBufferBuilder<'static>>>,
}

impl NodeClient {
    pub fn new(server_ip_port: SocketAddr) -> NodeClient {
        NodeClient {
            tcp_client: TcpClient::new(server_ip_port),
            response_buffer: ThreadLocal::new(),
            request_builder: ThreadLocal::new(),
        }
    }

    fn get_or_create_builder(&self) -> RefMut<FlatBufferBuilder<'static>> {
        let mut builder = self
            .request_builder
            .get_or(|| RefCell::new(FlatBufferBuilder::new()))
            .borrow_mut();
        builder.reset();
        return builder;
    }

    fn get_or_create_buffer(&self) -> RefMut<Vec<u8>> {
        return self
            .response_buffer
            .get_or(|| RefCell::new(vec![]))
            .borrow_mut();
    }

    fn send_receive_raw<'b>(
        &self,
        request: &[u8],
        buffer: &'b mut Vec<u8>,
    ) -> Result<&'b mut Vec<u8>, ErrorCode> {
        self.tcp_client
            .send_and_receive_length_prefixed(request, buffer.as_mut())
            .map_err(|_| ErrorCode::Uncategorized)?;
        Ok(buffer)
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

    pub fn filesystem_ready(&self) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let request_builder = FilesystemReadyRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::FilesystemReadyRequest,
            finish_offset,
        );

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
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
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        Ok(())
    }

    pub fn mkdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<FileAttr, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_name = builder.create_string(name);
        let mut request_builder = MkdirRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_uid(uid);
        request_builder.add_gid(gid);
        request_builder.add_mode(mode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::MkdirRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response
            .response_as_file_metadata_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn lookup(&self, parent: u64, name: &str, context: UserContext) -> Result<u64, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_name = builder.create_string(name);
        let mut request_builder = LookupRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::LookupRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let inode_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        inode_response
            .as_inode_response()
            .ok_or(ErrorCode::BadResponse)
    }

    pub fn create(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    ) -> Result<FileAttr, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_name = builder.create_string(name);
        let mut request_builder = CreateRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_uid(uid);
        request_builder.add_gid(gid);
        request_builder.add_mode(mode);
        request_builder.add_kind(kind);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::CreateRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response
            .response_as_file_metadata_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn statfs(&self) -> Result<StatFS, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let request_builder = FilesystemInformationRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::FilesystemInformationRequest,
            finish_offset,
        );

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();

        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let fs_info_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();
        if let ArchivedRkyvGenericResponse::FilesystemInformation {
            block_size,
            max_name_length,
        } = fs_info_response
        {
            return Ok(StatFS {
                block_size: block_size.into(),
                max_name_length: max_name_length.into(),
            });
        } else {
            return Err(ErrorCode::BadResponse);
        }
    }

    pub fn getattr(&self, inode: u64) -> Result<FileAttr, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let mut request_builder = GetattrRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::GetattrRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response
            .response_as_file_metadata_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn getxattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<Vec<u8>, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_key = builder.create_string(key);
        let mut request_builder = GetXattrRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_key(builder_key);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::GetXattrRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let data_response = rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data).unwrap();
        let data = data_response
            .as_read_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(data.to_vec());
    }

    pub fn listxattr(&self, inode: u64) -> Result<Vec<String>, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let mut request_builder = ListXattrsRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ListXattrsRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let xattrs_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        let xattrs = xattrs_response
            .as_xattrs_response()
            .ok_or(ErrorCode::BadResponse)?;

        let attrs = xattrs.iter().map(|x| x.to_string()).collect();

        return Ok(attrs);
    }

    pub fn setxattr(
        &self,
        inode: u64,
        key: &str,
        value: &[u8],
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_key = builder.create_string(key);
        let builder_value = builder.create_vector_direct(value);
        let mut request_builder = SetXattrRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_key(builder_key);
        request_builder.add_value(builder_value);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::SetXattrRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        Ok(())
    }

    pub fn removexattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_key = builder.create_string(key);
        let mut request_builder = RemoveXattrRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_key(builder_key);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RemoveXattrRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        Ok(())
    }

    pub fn utimens(
        &self,
        inode: u64,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let mut request_builder = UtimensRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        if let Some(ref atime) = atime {
            request_builder.add_atime(atime);
        }
        if let Some(ref mtime) = mtime {
            request_builder.add_mtime(mtime);
        }
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::UtimensRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn chmod(&self, inode: u64, mode: u32, context: UserContext) -> Result<(), ErrorCode> {
        if inode == ROOT_INODE {
            return Err(ErrorCode::OperationNotPermitted);
        }

        let mut builder = self.get_or_create_builder();
        let mut request_builder = ChmodRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_mode(mode);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ChmodRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn chown(
        &self,
        inode: u64,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let mut request_builder = ChownRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        let uid_struct;
        let gid_struct;
        if let Some(uid) = uid {
            uid_struct = OptionalUInt::new(uid);
            request_builder.add_uid(&uid_struct);
        }
        if let Some(gid) = gid {
            gid_struct = OptionalUInt::new(gid);
            request_builder.add_gid(&gid_struct);
        }
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ChownRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn hardlink(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<FileAttr, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let builder_new_name = builder.create_string(new_name);
        let mut request_builder = HardlinkRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_new_parent(new_parent);
        request_builder.add_new_name(builder_new_name);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::HardlinkRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let metadata = response
            .response_as_file_metadata_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(metadata_to_fuse_fileattr(&metadata));
    }

    pub fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_name = builder.create_string(name);
        let builder_new_name = builder.create_string(new_name);
        let mut request_builder = RenameRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_new_parent(new_parent);
        request_builder.add_new_name(builder_new_name);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RenameRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn readlink(&self, inode: u64) -> Result<Vec<u8>, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_offset(0);
        // TODO: this just tries to read a value longer than the longest link.
        // instead we should be using a special readlink message
        request_builder.add_read_size(999_999);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send_receive_raw(builder.finished_data(), &mut buffer)?;
        decode_fast_read_response_inplace(response).map(Clone::clone)
    }

    pub fn read<F: FnOnce(Result<&[u8], ErrorCode>)>(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        callback: F,
    ) {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        match self.send_receive_raw(builder.finished_data(), &mut buffer) {
            Ok(response) => match decode_fast_read_response_inplace(response) {
                Ok(data) => {
                    callback(Ok(data));
                    return;
                }
                Err(e) => {
                    callback(Err(e));
                    return;
                }
            },
            Err(e) => {
                callback(Err(e));
                return;
            }
        };
    }

    pub fn read_to_vec(&self, inode: u64, offset: u64, size: u32) -> Result<Vec<u8>, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = Vec::with_capacity((size + 1) as usize);
        self.send_receive_raw(builder.finished_data(), &mut buffer)?;
        decode_fast_read_response_inplace(&mut buffer)?;

        Ok(buffer)
    }

    pub fn readdir(&self, inode: u64) -> Result<Vec<(u64, OsString, fuser::FileType)>, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let mut request_builder = ReaddirRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReaddirRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;

        let mut result = vec![];
        let listing_response = response
            .response_as_directory_listing_response()
            .ok_or(ErrorCode::BadResponse)?;
        let entries = listing_response.entries();
        for i in 0..entries.len() {
            let entry = entries.get(i);
            result.push((
                entry.inode(),
                OsString::from(entry.name()),
                to_fuse_file_type(entry.kind()),
            ));
        }

        return Ok(result);
    }

    pub fn truncate(&self, inode: u64, length: u64, context: UserContext) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let mut builder = self.get_or_create_builder();
        let mut request_builder = TruncateRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_new_length(length);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::TruncateRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn write(&self, inode: u64, data: &[u8], offset: u64) -> Result<u32, ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let data_offset = builder.create_vector_direct(data);
        let mut request_builder = WriteRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        request_builder.add_offset(offset);
        request_builder.add_data(data_offset);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::WriteRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let written_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        written_response
            .as_bytes_written_response()
            .ok_or(ErrorCode::BadResponse)
    }

    pub fn fsync(&self, inode: u64) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let mut request_builder = FsyncRequestBuilder::new(&mut builder);
        request_builder.add_inode(inode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::FsyncRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn unlink(&self, parent: u64, name: &str, context: UserContext) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_name = builder.create_string(name);
        let mut request_builder = UnlinkRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::UnlinkRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }

    pub fn rmdir(&self, parent: u64, name: &str, context: UserContext) -> Result<(), ErrorCode> {
        let mut builder = self.get_or_create_builder();
        let builder_name = builder.create_string(name);
        let mut request_builder = RmdirRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RmdirRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(builder.finished_data(), &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_empty_response()
            .ok_or(ErrorCode::BadResponse)?;

        return Ok(());
    }
}
