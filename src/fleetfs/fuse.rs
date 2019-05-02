use std::ffi::{OsStr, OsString};
use std::net::SocketAddr;
use std::path::Path;

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use fuse_mt;
use fuse_mt::{CreatedEntry, DirectoryEntry, FileAttr, FilesystemMT, RequestInfo, ResultCreate, ResultData, ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultStatfs, ResultWrite, ResultXattr};
use libc;
use log::debug;
use log::warn;
use reqwest;
use reqwest::{Client, Url};
use time::Timespec;

use crate::fleetfs::core::PATH_HEADER;
use crate::fleetfs::generated::*;
use crate::fleetfs::tcp_client::TcpClient;

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

pub struct NodeClient {
    server_url: String,
    client: Client,
    tcp_client: TcpClient
}

impl NodeClient {
    pub fn new(server_url: &String, server_v2_ip_port: &SocketAddr) -> NodeClient {
        NodeClient {
            server_url: server_url.clone(),
            client: Client::new(),
            tcp_client: TcpClient::new(server_v2_ip_port.clone())
        }
    }

    pub fn mkdir(&self, filename: &String, mode: u16) -> Result<Option<FileAttr>, std::io::Error> {
        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(filename.as_str());
        let mut request_builder = MkdirRequestBuilder::new(&mut builder);
        request_builder.add_filename(builder_path);
        request_builder.add_mode(mode);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::MkdirRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        match response.response_type() {
            ResponseType::ErrorResponse => {
                let error = response.response_as_error_response().unwrap();
                // TODO
                assert_eq!(error.error_code(), ErrorCode::DoesNotExist);
                return Ok(None);
            },
            ResponseType::FileMetadataResponse => {
                let metadata = response.response_as_file_metadata_response().unwrap();

                let kind = match metadata.kind() {
                    FileType::File => fuse_mt::FileType::RegularFile,
                    FileType::Directory => fuse_mt::FileType::Directory,
                    FileType::DefaultValueNotAType => unreachable!()
                };

                return Ok(Some(FileAttr {
                    size: metadata.size_bytes(),
                    blocks: metadata.size_blocks(),
                    atime: Timespec {sec: metadata.last_access_time().seconds(), nsec: metadata.last_access_time().nanos()},
                    mtime: Timespec {sec: metadata.last_modified_time().seconds(), nsec: metadata.last_modified_time().nanos()},
                    ctime: Timespec {sec: metadata.last_metadata_modified_time().seconds(), nsec: metadata.last_metadata_modified_time().nanos()},
                    crtime: Timespec { sec: 0, nsec: 0 },
                    kind,
                    perm: metadata.mode(),
                    nlink: metadata.hard_links(),
                    uid: metadata.user_id(),
                    gid: metadata.group_id(),
                    rdev: metadata.device_id(),
                    flags: 0
                }));
            },
            _ => unimplemented!()
        }
    }

    pub fn getattr(&self, filename: &String) -> Result<Option<FileAttr>, std::io::Error> {
        if filename.len() == 1 {
            return Ok(Some(FileAttr {
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
            }));
        }

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(filename.as_str());
        let mut request_builder = GetattrRequestBuilder::new(&mut builder);
        request_builder.add_filename(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::GetattrRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        match response.response_type() {
            ResponseType::ErrorResponse => {
                let error = response.response_as_error_response().unwrap();
                // TODO
                assert_eq!(error.error_code(), ErrorCode::DoesNotExist);
                return Ok(None);
            },
            ResponseType::FileMetadataResponse => {
                let metadata = response.response_as_file_metadata_response().unwrap();

                let kind = match metadata.kind() {
                    FileType::File => fuse_mt::FileType::RegularFile,
                    FileType::Directory => fuse_mt::FileType::Directory,
                    FileType::DefaultValueNotAType => unreachable!()
                };

                return Ok(Some(FileAttr {
                    size: metadata.size_bytes(),
                    blocks: metadata.size_blocks(),
                    atime: Timespec {sec: metadata.last_access_time().seconds(), nsec: metadata.last_access_time().nanos()},
                    mtime: Timespec {sec: metadata.last_modified_time().seconds(), nsec: metadata.last_modified_time().nanos()},
                    ctime: Timespec {sec: metadata.last_metadata_modified_time().seconds(), nsec: metadata.last_metadata_modified_time().nanos()},
                    crtime: Timespec { sec: 0, nsec: 0 },
                    kind,
                    perm: metadata.mode(),
                    nlink: metadata.hard_links(),
                    uid: metadata.user_id(),
                    gid: metadata.group_id(),
                    rdev: metadata.device_id(),
                    flags: 0
                }));
            },
            _ => unimplemented!()
        }
    }

    pub fn utimens(&self, path: &String, atime_secs: i64, atime_nanos: i32, mtime_secs: i64, mtime_nanos: i32, forward: bool) -> Result<(), std::io::Error> {
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

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn chmod(&self, path: &String, mode: u32, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ChmodRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_mode(mode);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ChmodRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn hardlink(&self, path: &String, new_path: &String, forward: bool) -> Result<Option<FileAttr>, std::io::Error> {
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

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        match response.response_type() {
            ResponseType::ErrorResponse => {
                let error = response.response_as_error_response().unwrap();
                // TODO
                assert_eq!(error.error_code(), ErrorCode::DoesNotExist);
                return Ok(None);
            },
            ResponseType::FileMetadataResponse => {
                let metadata = response.response_as_file_metadata_response().unwrap();

                let kind = match metadata.kind() {
                    FileType::File => fuse_mt::FileType::RegularFile,
                    FileType::Directory => fuse_mt::FileType::Directory,
                    FileType::DefaultValueNotAType => unreachable!()
                };

                return Ok(Some(FileAttr {
                    size: metadata.size_bytes(),
                    blocks: metadata.size_blocks(),
                    atime: Timespec {sec: metadata.last_access_time().seconds(), nsec: metadata.last_access_time().nanos()},
                    mtime: Timespec {sec: metadata.last_modified_time().seconds(), nsec: metadata.last_modified_time().nanos()},
                    ctime: Timespec {sec: metadata.last_metadata_modified_time().seconds(), nsec: metadata.last_metadata_modified_time().nanos()},
                    crtime: Timespec { sec: 0, nsec: 0 },
                    kind,
                    perm: metadata.mode(),
                    nlink: metadata.hard_links(),
                    uid: metadata.user_id(),
                    gid: metadata.group_id(),
                    rdev: metadata.device_id(),
                    flags: 0
                }));
            },
            _ => unimplemented!()
        }
    }

    pub fn rename(&self, path: &String, new_path: &String, forward: bool) -> Result<(), std::io::Error> {
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

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn read(&self, path: &String, offset: u64, size: u32) -> Result<Vec<u8>, std::io::Error> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        request_builder.add_filename(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        let data = response.response_as_read_response().unwrap().data().to_vec();

        return Ok(data);
    }

    pub fn readdir(&self, path: &String) -> ResultReaddir {
        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ReaddirRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReaddirRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data()).map_err(|_| libc::EIO)?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        let mut result = vec![];
        let listing_response = response.response_as_directory_listing_response().unwrap();
        let entries = listing_response.entries();
        for i in 0..entries.len() {
            let entry = entries.get(i);
            result.push(DirectoryEntry {
                name: OsString::from(entry.filename()),
                kind: file_type_to_fuse_type(entry.kind())
            });
        }

        return Ok(result);
    }

    pub fn truncate(&self, path: &String, length: u64, forward: bool) -> Result<(), std::io::Error> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = TruncateRequestBuilder::new(&mut builder);
        request_builder.add_path(builder_path);
        request_builder.add_new_length(length);
        request_builder.add_forward(forward);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::TruncateRequest, finish_offset);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        response.response_as_empty_response().unwrap();

        return Ok(());
    }

    pub fn write(&self, path: &String, data: Vec<u8>, offset: u64) -> Result<(), reqwest::Error> {
        let uri: Url = format!("{}/{}", self.server_url, offset).parse().unwrap();
        return self.client.post(uri)
            .body(data)
            .header(PATH_HEADER, path.as_str())
            .send().map(|_| ());
    }

    pub fn unlink(&self, path: &String) -> Result<(), reqwest::Error> {
        let uri: Url = format!("{}", self.server_url).parse().unwrap();
        return self.client.delete(uri)
            .header(PATH_HEADER, path.as_str())
            .send().map(|_| ());
    }
}

pub struct FleetFUSE {
    client: NodeClient
}

impl FleetFUSE {
    pub fn new(server_url: String, server_ip_port: SocketAddr) -> FleetFUSE {
        FleetFUSE {
            client: NodeClient::new(&server_url, &server_ip_port)
        }
    }
}

impl FilesystemMT for FleetFUSE {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn destroy(&self, _req: RequestInfo) {
        // No-op
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        debug!("getattr() called with {:?}", path);
        let filename = path.to_str().unwrap().to_string();
        let result = match self.client.getattr(&filename).map_err(|_| libc::EIO)? {
            None => Err(libc::ENOENT),
            Some(fileattr) => Ok((Timespec { sec: 0, nsec: 0 }, fileattr)),
        };

        debug!("getattr() returned {:?}", &result);
        return result;
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, mode: u32) -> ResultEmpty {
        debug!("chmod() called with {:?}, {:?}", path, mode);
        return self.client.chmod(&path.to_str().unwrap().to_string(), mode, true).map_err(|_| libc::EIO);
    }

    fn chown(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _uid: Option<u32>, _gid: Option<u32>) -> ResultEmpty {
        warn!("chown() not implemented");
        Err(libc::ENOSYS)
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!("truncate() called with {:?}", path);
        let filename = path.to_str().unwrap().to_string();
        self.client.truncate(&filename, size, true).map_err(|_| libc::EIO)
    }

    fn utimens(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>) -> ResultEmpty {
        debug!("utimens() called with {:?}, {:?}, {:?}", path, atime, mtime);
        return self.client.utimens(&path.to_str().unwrap().to_string(),
                                   atime.map(|x| x.sec).unwrap_or(0),
                                   atime.map(|x| x.nsec).unwrap_or(0),
                                   mtime.map(|x| x.sec).unwrap_or(0),
                                   mtime.map(|x| x.nsec).unwrap_or(0),
                                   true).map_err(|_| libc::EIO);
    }

    fn readlink(&self, _req: RequestInfo, _path: &Path) -> ResultData {
        warn!("readlink() not implemented");
        Err(libc::ENOSYS)
    }

    fn mknod(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32, _rdev: u32) -> ResultEntry {
        warn!("mknod() not implemented");
        Err(libc::ENOSYS)
    }

    fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
        debug!("mkdir() called with {:?} {:?}", parent, name);
        let path = Path::new(parent).join(name);
        let result = match self.client.mkdir(&path.to_str().unwrap().to_string(), mode as u16).map_err(|_| libc::EIO)? {
            None => Err(libc::ENOENT),
            Some(fileattr) => Ok((Timespec { sec: 0, nsec: 0 }, fileattr)),
        };

        return result;
    }

    fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("unlink() called with {:?} {:?}", parent, name);
        let path = Path::new(parent).join(name);
        let path = path.to_str().unwrap().to_string();
        self.client.unlink(&path).map_err(|_| libc::EIO)
    }

    fn rmdir(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr) -> ResultEmpty {
        warn!("rmdir() not implemented");
        Err(libc::ENOSYS)
    }

    fn symlink(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _target: &Path) -> ResultEntry {
        warn!("symlink() not implemented");
        Err(libc::ENOSYS)
    }

    fn rename(&self, _req: RequestInfo, parent: &Path, name: &OsStr, new_parent: &Path, new_name: &OsStr) -> ResultEmpty {
        let path = Path::new(parent).join(name);
        let new_path = Path::new(new_parent).join(new_name);
        return self.client.rename(&path.to_str().unwrap().to_string(), &new_path.to_str().unwrap().to_string(), true).map_err(|_| libc::EIO);
    }

    fn link(&self, _req: RequestInfo, path: &Path, new_parent: &Path, new_name: &OsStr) -> ResultEntry {
        debug!("link() called for {:?}, {:?}, {:?}", path, new_parent, new_name);
        let new_path = Path::new(new_parent).join(new_name);
        let result = self.client.hardlink(&path.to_str().unwrap().to_string(), &new_path.to_str().unwrap().to_string(), true).map_err(|_| libc::EIO)?;

        debug!("getattr() returned {:?}", &result);
        return Ok((Timespec {sec: 0, nsec: 0}, result.unwrap()));
    }

    fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        debug!("open() called for {:?}", path);
        // TODO: something reasonable
        Ok((0, 0))
    }

    fn read(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, size: u32) -> ResultData {
        debug!("read() called on {:?} with offset={} and size={}", path, offset, size);
        let filename = path.to_str().unwrap().to_string();
        return self.client.read(&filename, offset, size).map_err(|_| libc::EIO);
    }

    fn write(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
        debug!("write() called with {:?}", path);
        let filename = path.to_str().unwrap().to_string();
        let len = data.len() as u32;
        match self.client.write(&filename, data, offset) {
            Ok(_) => Ok(len),
            Err(_) => Err(libc::EIO),
        }
    }

    fn flush(&self, _req: RequestInfo, path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        debug!("flush() called on {:?}", path);
        // TODO: something reasonable
        Ok(())
    }

    fn release(&self, _req: RequestInfo, path: &Path, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        debug!("release() called on {:?}", path);
        Ok(())
    }

    fn fsync(&self, _req: RequestInfo, _path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        warn!("fsync() not implemented");
        Err(libc::ENOSYS)
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        debug!("opendir() called on {:?}", path);
        Ok((0, 0))
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
        debug!("readdir() called with {:?}", path);
        let filename = path.to_str().unwrap().to_string();
        // TODO: when server is down return EIO
        let result = self.client.readdir(&filename);
        debug!("readdir() returned {:?}", &result);

        return result;
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir() called on {:?}", path);
        Ok(())
    }

    fn fsyncdir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        warn!("fsyncdir() not implemented");
        Err(libc::ENOSYS)
    }

    fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
        warn!("statfs() not implemented");
        Err(libc::ENOSYS)
    }

    fn setxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _value: &[u8], _flags: u32, _position: u32) -> ResultEmpty {
        warn!("setxattr() not implemented");
        Err(libc::ENOSYS)
    }

    fn getxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _size: u32) -> ResultXattr {
        warn!("getxattr() not implemented");
        Err(libc::ENOSYS)
    }

    fn listxattr(&self, _req: RequestInfo, _path: &Path, _size: u32) -> ResultXattr {
        warn!("listxattr() not implemented");
        Err(libc::ENOSYS)
    }

    fn removexattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr) -> ResultEmpty {
        warn!("removexattr() not implemented");
        Err(libc::ENOSYS)
    }

    fn access(&self, _req: RequestInfo, _path: &Path, _mask: u32) -> ResultEmpty {
        warn!("access() not implemented");
        Err(libc::ENOSYS)
    }

    fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, _mode: u32, _flags: u32) -> ResultCreate {
        debug!("create() called with {:?} {:?}", parent, name);
        // TODO: kind of a hack to create the file
        let path = Path::new(parent).join(name);
        match self.client.write(&path.to_str().unwrap().to_string(), vec![], 0) {
            Ok(_) => {},
            Err(_) => return Err(libc::EIO),
        };
        // TODO
        Ok(CreatedEntry {
            ttl: Timespec { sec: 0, nsec: 0 },
            attr: FileAttr {
                size: 0,
                blocks: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                crtime: Timespec { sec: 0, nsec: 0 },
                kind: fuse_mt::FileType::RegularFile,
                perm: 0,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0
            },
            fh: 0,
            flags: 0
        })
    }
}

