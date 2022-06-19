use std::cell::{RefCell, RefMut};
use std::ffi::OsString;
use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;
use thread_local::ThreadLocal;

use crate::base::fast_data_protocol::decode_fast_read_response_inplace;
use crate::base::message_types::{
    ArchivedRkyvGenericResponse, EntryMetadata, ErrorCode, FileKind, RkyvGenericResponse,
    RkyvRequest, Timestamp, UserContext,
};
use crate::base::{finalize_request_without_prefix, response_or_error};
use crate::client::tcp_client::TcpClient;
use crate::generated::*;
use crate::storage::ROOT_INODE;
use byteorder::{ByteOrder, LittleEndian};
use fuser::FileAttr;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::AlignedVec;
use std::time::SystemTime;

fn to_fuse_file_type(file_type: FileKind) -> fuser::FileType {
    match file_type {
        FileKind::File => fuser::FileType::RegularFile,
        FileKind::Directory => fuser::FileType::Directory,
        FileKind::Symlink => fuser::FileType::Symlink,
    }
}

pub struct StatFS {
    pub block_size: u32,
    pub max_name_length: u32,
}

fn metadata_to_fuse_fileattr(metadata: &EntryMetadata) -> FileAttr {
    FileAttr {
        ino: metadata.inode,
        size: metadata.size_bytes,
        blocks: metadata.size_blocks,
        atime: metadata.last_access_time.into(),
        mtime: metadata.last_modified_time.into(),
        ctime: metadata.last_metadata_modified_time.into(),
        crtime: SystemTime::UNIX_EPOCH,
        kind: to_fuse_file_type(metadata.kind),
        perm: metadata.mode,
        nlink: metadata.hard_links,
        uid: metadata.user_id,
        gid: metadata.group_id,
        rdev: metadata.device_id,
        flags: 0,
        blksize: metadata.block_size,
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

    fn send_flatbuffer_receive_raw<'b>(
        &self,
        request: &[u8],
        buffer: &'b mut Vec<u8>,
    ) -> Result<&'b mut Vec<u8>, ErrorCode> {
        let request = RkyvRequest::Flatbuffer(request.as_ref().to_vec());
        let mut serializer = AllocSerializer::<64>::default();
        serializer.serialize_value(&request).unwrap();
        let request_buffer = serializer.into_serializer().into_inner();
        let mut send_buffer = vec![0; request_buffer.len() + 4];
        LittleEndian::write_u32(&mut send_buffer, request_buffer.len() as u32);
        // TODO: optimize out this copy
        send_buffer[4..].copy_from_slice(&request_buffer);
        self.tcp_client
            .send_and_receive_length_prefixed(&send_buffer, buffer.as_mut())
            .map_err(|_| ErrorCode::Uncategorized)?;
        Ok(buffer)
    }

    fn send_flatbuffer<'b>(
        &self,
        request: &[u8],
        buffer: &'b mut Vec<u8>,
    ) -> Result<GenericResponse<'b>, ErrorCode> {
        let request = RkyvRequest::Flatbuffer(request.as_ref().to_vec());
        self.send(request, buffer)
    }

    fn send<'b>(
        &self,
        request: RkyvRequest,
        buffer: &'b mut Vec<u8>,
    ) -> Result<GenericResponse<'b>, ErrorCode> {
        // TODO: reuse these serializers to reduce allocations, like get_or_create_builder()
        let mut serializer = AllocSerializer::<64>::default();
        serializer.serialize_value(&request).unwrap();
        let request_buffer = serializer.into_serializer().into_inner();
        let mut send_buffer = vec![0; request_buffer.len() + 4];
        LittleEndian::write_u32(&mut send_buffer, request_buffer.len() as u32);
        // TODO: optimize out this copy
        send_buffer[4..].copy_from_slice(&request_buffer);
        self.tcp_client
            .send_and_receive_length_prefixed(&send_buffer, buffer.as_mut())
            .map_err(|_| ErrorCode::Uncategorized)?;
        return response_or_error(buffer);
    }

    pub fn filesystem_ready(&self) -> Result<(), ErrorCode> {
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::FilesystemReady, &mut buffer)?;
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
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::FilesystemCheck, &mut buffer)?;
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
        let request = RkyvRequest::Mkdir {
            parent,
            name: name.to_string(),
            uid,
            gid,
            mode,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();

        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let attr_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        return Ok(metadata_to_fuse_fileattr(
            &attr_response.as_attr_response().unwrap(),
        ));
    }

    pub fn lookup(&self, parent: u64, name: &str, context: UserContext) -> Result<u64, ErrorCode> {
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(
            RkyvRequest::Lookup {
                parent,
                name: name.to_string(),
                context,
            },
            &mut buffer,
        )?;
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
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(
            RkyvRequest::Create {
                parent,
                name: name.to_string(),
                uid,
                gid,
                mode,
                kind,
            },
            &mut buffer,
        )?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();

        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let attr_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        return Ok(metadata_to_fuse_fileattr(
            &attr_response.as_attr_response().unwrap(),
        ));
    }

    pub fn statfs(&self) -> Result<StatFS, ErrorCode> {
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::FilesystemInformation, &mut buffer)?;
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
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::GetAttr { inode }, &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();

        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let attr_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        return Ok(metadata_to_fuse_fileattr(
            &attr_response.as_attr_response().unwrap(),
        ));
    }

    pub fn getxattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<Vec<u8>, ErrorCode> {
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(
            RkyvRequest::GetXattr {
                inode,
                key: key.to_string(),
                context,
            },
            &mut buffer,
        )?;
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
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::ListXattrs { inode }, &mut buffer)?;
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
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(
            RkyvRequest::SetXattr {
                inode,
                key: key.to_string(),
                value: value.to_vec(),
                context,
            },
            &mut buffer,
        )?;
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
        let request = RkyvRequest::RemoveXattr {
            inode,
            key: key.to_string(),
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        let request = RkyvRequest::Utimens {
            inode,
            atime,
            mtime,
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        let request = RkyvRequest::Chmod {
            inode,
            mode,
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        let request = RkyvRequest::Chown {
            inode,
            uid,
            gid,
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        let request = RkyvRequest::Hardlink {
            inode,
            new_parent,
            new_name: new_name.to_string(),
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();

        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let attr_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        return Ok(metadata_to_fuse_fileattr(
            &attr_response.as_attr_response().unwrap(),
        ));
    }

    pub fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let request = RkyvRequest::Rename {
            parent,
            name: name.to_string(),
            new_parent,
            new_name: new_name.to_string(),
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        finalize_request_without_prefix(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send_flatbuffer_receive_raw(builder.finished_data(), &mut buffer)?;
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
        finalize_request_without_prefix(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        match self.send_flatbuffer_receive_raw(builder.finished_data(), &mut buffer) {
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
        finalize_request_without_prefix(&mut builder, RequestType::ReadRequest, finish_offset);

        let mut buffer = Vec::with_capacity((size + 1) as usize);
        self.send_flatbuffer_receive_raw(builder.finished_data(), &mut buffer)?;
        decode_fast_read_response_inplace(&mut buffer)?;

        Ok(buffer)
    }

    pub fn readdir(&self, inode: u64) -> Result<Vec<(u64, OsString, fuser::FileType)>, ErrorCode> {
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::ListDir { inode }, &mut buffer)?;

        let mut result = vec![];
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let entries = rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
            .unwrap()
            .as_directory_listing_response()
            .ok_or(ErrorCode::BadResponse)?;
        for entry in entries {
            result.push((
                entry.inode,
                OsString::from(entry.name),
                to_fuse_file_type(entry.kind),
            ));
        }

        return Ok(result);
    }

    pub fn truncate(&self, inode: u64, length: u64, context: UserContext) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        let request = RkyvRequest::Truncate {
            inode,
            new_length: length,
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        finalize_request_without_prefix(&mut builder, RequestType::WriteRequest, finish_offset);

        let mut buffer = self.get_or_create_buffer();
        let response = self.send_flatbuffer(builder.finished_data(), &mut buffer)?;
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
        let mut buffer = self.get_or_create_buffer();
        let response = self.send(RkyvRequest::Fsync { inode }, &mut buffer)?;
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
        let request = RkyvRequest::Unlink {
            parent,
            name: name.to_string(),
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
        let request = RkyvRequest::Rmdir {
            parent,
            name: name.to_string(),
            context,
        };

        let mut buffer = self.get_or_create_buffer();
        let response = self.send(request, &mut buffer)?;
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
