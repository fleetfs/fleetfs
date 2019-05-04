use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::Path;

use fuse_mt;
use fuse_mt::{CreatedEntry, FileAttr, FilesystemMT, RequestInfo, ResultCreate, ResultData, ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultStatfs, ResultWrite, ResultXattr};
use libc;
use log::debug;
use log::warn;
use time::Timespec;

use crate::client::NodeClient;
use crate::generated::ErrorCode;
use std::os::raw::c_int;

pub struct FleetFUSE {
    client: NodeClient
}

impl FleetFUSE {
    pub fn new(server_ip_port: SocketAddr) -> FleetFUSE {
        FleetFUSE {
            client: NodeClient::new(&server_ip_port)
        }
    }
}

fn into_fuse_error(error: ErrorCode) -> c_int {
    match error {
        ErrorCode::DoesNotExist => libc::ENOENT,
        ErrorCode::Uncategorized => libc::EIO,
        ErrorCode::DefaultValueNotAnError => unreachable!(),
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
        // TODO
        if path.to_str().unwrap().len() == 1 {
            return Ok((Timespec {sec: 0, nsec: 0}, FileAttr {
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

        let path = path.to_str().unwrap().to_string();
        return self.client.getattr(&path)
            .map(|file_attr| (Timespec {sec: 0, nsec: 0}, file_attr))
            .map_err(|e| into_fuse_error(e));
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, mode: u32) -> ResultEmpty {
        debug!("chmod() called with {:?}, {:?}", path, mode);
        return self.client.chmod(&path.to_str().unwrap().to_string(), mode, true).map_err(|e| into_fuse_error(e));
    }

    fn chown(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _uid: Option<u32>, _gid: Option<u32>) -> ResultEmpty {
        warn!("chown() not implemented");
        Err(libc::ENOSYS)
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!("truncate() called with {:?}", path);
        let path = path.to_str().unwrap().to_string();
        self.client.truncate(&path, size, true).map_err(|e| into_fuse_error(e))
    }

    fn utimens(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>) -> ResultEmpty {
        debug!("utimens() called with {:?}, {:?}, {:?}", path, atime, mtime);
        return self.client.utimens(&path.to_str().unwrap().to_string(),
                                   atime.map(|x| x.sec).unwrap_or(0),
                                   atime.map(|x| x.nsec).unwrap_or(0),
                                   mtime.map(|x| x.sec).unwrap_or(0),
                                   mtime.map(|x| x.nsec).unwrap_or(0),
                                   true).map_err(|e| into_fuse_error(e));
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
        return self.client.mkdir(&path.to_str().unwrap().to_string(), mode as u16, true)
            .map(|file_attr| (Timespec {sec: 0, nsec: 0}, file_attr))
            .map_err(|e| into_fuse_error(e));
    }

    fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("unlink() called with {:?} {:?}", parent, name);
        let path = Path::new(parent).join(name);
        let path = path.to_str().unwrap().to_string();
        self.client.unlink(&path, true).map_err(|e| into_fuse_error(e))
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
        return self.client.rename(&path.to_str().unwrap().to_string(), &new_path.to_str().unwrap().to_string(), true).map_err(|e| into_fuse_error(e));
    }

    fn link(&self, _req: RequestInfo, path: &Path, new_parent: &Path, new_name: &OsStr) -> ResultEntry {
        debug!("link() called for {:?}, {:?}, {:?}", path, new_parent, new_name);
        let new_path = Path::new(new_parent).join(new_name);
        return self.client.hardlink(&path.to_str().unwrap().to_string(), &new_path.to_str().unwrap().to_string(), true)
            .map(|file_attr| (Timespec {sec: 0, nsec: 0}, file_attr))
            .map_err(|e| into_fuse_error(e));
    }

    fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        debug!("open() called for {:?}", path);
        // TODO: something reasonable
        Ok((0, 0))
    }

    fn read(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, size: u32) -> ResultData {
        debug!("read() called on {:?} with offset={} and size={}", path, offset, size);
        let path = path.to_str().unwrap().to_string();
        return self.client.read(&path, offset, size).map_err(|e| into_fuse_error(e));
    }

    fn write(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
        debug!("write() called with {:?}", path);
        let path = path.to_str().unwrap().to_string();
        return self.client.write(&path, &data, offset, true).map_err(|e| into_fuse_error(e));
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
        let path = path.to_str().unwrap().to_string();
        return self.client.readdir(&path).map_err(|e| into_fuse_error(e));
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
        self.client.write(&path.to_str().unwrap().to_string(), &vec![], 0, true).map_err(|e| into_fuse_error(e))?;
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

