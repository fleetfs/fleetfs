use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::net::{SocketAddr};
use std::path::Path;

use crate::fleetfs::generated::*;

use fuse_mt::{CreatedEntry, DirectoryEntry, FileAttr, FilesystemMT, FileType, RequestInfo, ResultCreate, ResultData, ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultStatfs, ResultWrite, ResultXattr};
use libc;
use log::debug;
use log::warn;
use reqwest;
use reqwest::{Client, Url};
use time::Timespec;

use crate::fleetfs::core::{PATH_HEADER};
use flatbuffers::FlatBufferBuilder;
use crate::fleetfs::tcp_client::TcpClient;

struct NodeClient {
    server_url: String,
    client: Client,
    tcp_client: TcpClient,
    getattr_url: Url,
    read_url: Url
}

impl NodeClient {
    pub fn new(server_url: &String, server_v2_ip_port: &SocketAddr) -> NodeClient {
        NodeClient {
            server_url: server_url.clone(),
            client: Client::new(),
            tcp_client: TcpClient::new(server_v2_ip_port.clone()),
            getattr_url: format!("{}/getattr", server_url).parse().unwrap(),
            read_url: format!("{}/", server_url).parse().unwrap()
        }
    }

    pub fn getattr(&self, filename: &String) -> Result<Option<FileAttr>, reqwest::Error> {
        if filename.len() == 1 {
            return Ok(Some(FileAttr {
                size: 0,
                blocks: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                crtime: Timespec { sec: 0, nsec: 0 },
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0
            }));
        }

        let mut response = self.client
            .get(self.getattr_url.clone())
            .header(PATH_HEADER, filename.as_str())
            .send()?;
        let resp: HashMap<String, u64> = response.json()?;

        let exists = *resp.get("exists").expect("Server returned bad response: exists field missing");
        if exists != 0 {
            return Ok(Some(FileAttr {
                size: *resp.get("length").expect("Server returned a corrupted response: length field missing"),
                blocks: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                crtime: Timespec { sec: 0, nsec: 0 },
                kind: FileType::RegularFile,
                perm: 0o777,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0
            }));
        }
        else {
            return Ok(None);
        }

    }

    pub fn read(&self, path: &String, offset: u64, size: u32) -> Result<Vec<u8>, std::io::Error> {
        assert_ne!(path, "/");

        let mut builder = FlatBufferBuilder::new();
        let builder_path = builder.create_string(path.as_str());
        let mut request_builder = ReadRequestBuilder::new(&mut builder);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        request_builder.add_filename(builder_path);
        let finish_offset = request_builder.finish();
        let mut generic_request_builder = GenericRequestBuilder::new(&mut builder);
        generic_request_builder.add_request_type(RequestType::ReadRequest);
        generic_request_builder.add_request(finish_offset.as_union_value());
        let finish_offset = generic_request_builder.finish();
        builder.finish_size_prefixed(finish_offset, None);

        let buffer = self.tcp_client.send_and_receive_length_prefixed(builder.finished_data())?;
        let response = flatbuffers::get_root::<GenericResponse>(&buffer);
        let data = response.response_as_read_response().unwrap().data().to_vec();

        return Ok(data);
    }

    pub fn readdir(&self, path: &String) -> ResultReaddir {
        assert_eq!(path, "/");
        let response: Vec<String> = self.client.get(self.read_url.clone())
            .header(PATH_HEADER, path.as_str())
            .send().unwrap().json().unwrap();

        let mut result = vec![];
        for file in response {
            result.push(DirectoryEntry {
                name: OsString::from(file),
                // TODO: support other file types
                kind: FileType::RegularFile
            });
        }

        return Ok(result);
    }

    pub fn truncate(&self, path: &String, length: u64) -> Result<(), reqwest::Error> {
        let uri: Url = format!("{}/truncate/{}", self.server_url, length).parse().unwrap();
        return self.client.post(uri)
            .header(PATH_HEADER, path.as_str())
            .send().map(|_| ());
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

    fn chmod(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _mode: u32) -> ResultEmpty {
        warn!("chmod() not implemented");
        Err(libc::ENOSYS)
    }

    fn chown(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _uid: Option<u32>, _gid: Option<u32>) -> ResultEmpty {
        warn!("chown() not implemented");
        Err(libc::ENOSYS)
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!("truncate() called with {:?}", path);
        let filename = path.to_str().unwrap().to_string();
        self.client.truncate(&filename, size).map_err(|_| libc::EIO)
    }

    fn utimens(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>) -> ResultEmpty {
        warn!("utimens() not implemented");
        Err(libc::ENOSYS)
    }

    fn readlink(&self, _req: RequestInfo, _path: &Path) -> ResultData {
        warn!("readlink() not implemented");
        Err(libc::ENOSYS)
    }

    fn mknod(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32, _rdev: u32) -> ResultEntry {
        warn!("mknod() not implemented");
        Err(libc::ENOSYS)
    }

    fn mkdir(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32) -> ResultEntry {
        warn!("mkdir() not implemented");
        Err(libc::ENOSYS)
    }

    fn unlink(&self, _req: RequestInfo, _parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("unlink() called with {:?}", name);
        let filename = name.to_str().unwrap().to_string();
        self.client.unlink(&filename).map_err(|_| libc::EIO)
    }

    fn rmdir(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr) -> ResultEmpty {
        warn!("rmdir() not implemented");
        Err(libc::ENOSYS)
    }

    fn symlink(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _target: &Path) -> ResultEntry {
        warn!("symlink() not implemented");
        Err(libc::ENOSYS)
    }

    fn rename(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _newparent: &Path, _newname: &OsStr) -> ResultEmpty {
        warn!("rename() not implemented");
        Err(libc::ENOSYS)
    }

    fn link(&self, _req: RequestInfo, _path: &Path, _newparent: &Path, _newname: &OsStr) -> ResultEntry {
        warn!("link() not implemented");
        Err(libc::ENOSYS)
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
        // TODO: handle parent correctly
        let filename = name.to_str().unwrap().to_string();
        match self.client.write(&filename, vec![], 0) {
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
                kind: FileType::RegularFile,
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

