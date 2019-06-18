use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::Path;

use fuse_mt;
use fuse_mt::{
    CreatedEntry, FilesystemMT, RequestInfo, ResultCreate, ResultData, ResultEmpty, ResultEntry,
    ResultOpen, ResultReaddir, ResultStatfs, ResultWrite, ResultXattr, Statfs, Xattr,
};
use libc;
use log::debug;
use log::warn;
use time::Timespec;

use crate::client::NodeClient;
use crate::generated::ErrorCode;
use crate::storage::ROOT_INODE;
use std::os::raw::c_int;

pub struct FleetFUSE {
    client: NodeClient,
}

impl FleetFUSE {
    pub fn new(server_ip_port: SocketAddr) -> FleetFUSE {
        FleetFUSE {
            client: NodeClient::new(server_ip_port),
        }
    }
}

fn into_fuse_error(error: ErrorCode) -> c_int {
    match error {
        ErrorCode::DoesNotExist => libc::ENOENT,
        ErrorCode::Uncategorized => libc::EIO,
        ErrorCode::Corrupted => libc::EIO,
        ErrorCode::RaftFailure => libc::EIO,
        ErrorCode::FileTooLarge => libc::EFBIG,
        ErrorCode::AccessDenied => libc::EACCES,
        ErrorCode::OperationNotPermitted => libc::EPERM,
        ErrorCode::DefaultValueNotAnError => unreachable!(),
    }
}

impl FleetFUSE {
    fn lookup_path(&self, path: &str) -> Result<u64, libc::c_int> {
        let mut components = Path::new(path).components();
        // Skip root
        components.next();
        let mut inode = ROOT_INODE;
        for component in components {
            let name = component.as_os_str().to_str().unwrap().to_string();
            inode = self.client.lookup(inode, &name).map_err(into_fuse_error)?;
        }

        Ok(inode)
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
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        return self
            .client
            .getattr(inode)
            .map(|file_attr| (Timespec { sec: 0, nsec: 0 }, file_attr))
            .map_err(into_fuse_error);
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, mode: u32) -> ResultEmpty {
        debug!("chmod() called with {:?}, {:?}", path, mode);
        return self
            .client
            .chmod(path.to_str().unwrap(), mode)
            .map_err(into_fuse_error);
    }

    fn chown(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ResultEmpty {
        debug!("chown() called with {:?} {:?} {:?}", path, uid, gid);
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        self.client.chown(inode, uid, gid).map_err(into_fuse_error)
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!("truncate() called with {:?}", path);
        let path = path.to_str().unwrap();
        self.client.truncate(path, size).map_err(into_fuse_error)
    }

    fn utimens(
        &self,
        req: RequestInfo,
        path: &Path,
        _fh: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
    ) -> ResultEmpty {
        debug!("utimens() called with {:?}, {:?}, {:?}", path, atime, mtime);
        let inode = self.lookup_path(path.to_str().unwrap())?;
        return self
            .client
            .utimens(
                inode,
                req.uid,
                atime.map(|x| x.sec).unwrap_or(0),
                atime.map(|x| x.nsec).unwrap_or(libc::UTIME_NOW as i32),
                mtime.map(|x| x.sec).unwrap_or(0),
                mtime.map(|x| x.nsec).unwrap_or(libc::UTIME_NOW as i32),
            )
            .map_err(into_fuse_error);
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        debug!("read() called on {:?}", path);
        self.client
            .readlink(path.to_str().unwrap())
            .map_err(into_fuse_error)
    }

    fn mknod(
        &self,
        req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        mode: u32,
        _rdev: u32,
    ) -> ResultEntry {
        if (mode & libc::S_IFREG) == 0 {
            // TODO
            warn!("mknod() implementation is incomplete. Only supports regular files");
            Err(libc::ENOSYS)
        } else {
            self.create(req, parent, name, mode, 0)
                .map(|created| (created.ttl, created.attr))
        }
    }

    fn mkdir(&self, req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
        debug!("mkdir() called with {:?} {:?}", parent, name);
        let path = Path::new(parent).join(name);
        return self
            .client
            .mkdir(path.to_str().unwrap(), req.uid, req.gid, mode as u16)
            .map(|file_attr| (Timespec { sec: 0, nsec: 0 }, file_attr))
            .map_err(into_fuse_error);
    }

    fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("unlink() called with {:?} {:?}", parent, name);
        let path = Path::new(parent).join(name);
        let path = path.to_str().unwrap();
        self.client.unlink(path).map_err(into_fuse_error)
    }

    fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("rmdir() called with {:?} {:?}", parent, name);
        let path = Path::new(parent).join(name);
        let path = path.to_str().unwrap();
        self.client.rmdir(path).map_err(into_fuse_error)
    }

    fn symlink(&self, req: RequestInfo, parent: &Path, name: &OsStr, target: &Path) -> ResultEntry {
        debug!("symlink() called with {:?} {:?} {:?}", parent, name, target);
        let path = Path::new(parent).join(name);
        self.truncate(req, &path, None, 0)?;
        self.write(
            req,
            &path,
            0,
            0,
            Vec::from(target.to_str().unwrap().to_string()),
            0,
        )?;
        self.getattr(req, &path, None)
    }

    fn rename(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        new_parent: &Path,
        new_name: &OsStr,
    ) -> ResultEmpty {
        let path = Path::new(parent).join(name);
        let new_path = Path::new(new_parent).join(new_name);
        return self
            .client
            .rename(path.to_str().unwrap(), new_path.to_str().unwrap())
            .map_err(into_fuse_error);
    }

    fn link(
        &self,
        _req: RequestInfo,
        path: &Path,
        new_parent: &Path,
        new_name: &OsStr,
    ) -> ResultEntry {
        debug!(
            "link() called for {:?}, {:?}, {:?}",
            path, new_parent, new_name
        );
        let new_path = Path::new(new_parent).join(new_name);
        return self
            .client
            .hardlink(path.to_str().unwrap(), new_path.to_str().unwrap())
            .map(|file_attr| (Timespec { sec: 0, nsec: 0 }, file_attr))
            .map_err(into_fuse_error);
    }

    fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        debug!("open() called for {:?}", path);
        // TODO: something reasonable
        Ok((0, 0))
    }

    fn read(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: u64,
        offset: u64,
        size: u32,
        reply: impl FnOnce(Result<&[u8], libc::c_int>),
    ) {
        debug!(
            "read() called on {:?} with offset={} and size={}",
            path, offset, size
        );
        let path = path.to_str().unwrap();
        self.client.read(path, offset, size, move |result| {
            reply(result.map_err(into_fuse_error));
        });
    }

    fn write(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: u64,
        offset: u64,
        data: Vec<u8>,
        _flags: u32,
    ) -> ResultWrite {
        debug!("write() called with {:?}", path);
        let path = path.to_str().unwrap();
        let response = self
            .client
            .write(path, &data, offset)
            .map_err(into_fuse_error);
        debug!("write() response {:?}", response);
        response
    }

    fn flush(&self, _req: RequestInfo, path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        debug!("flush() called on {:?}", path);
        // TODO: something reasonable
        Ok(())
    }

    fn release(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> ResultEmpty {
        debug!("release() called on {:?}", path);
        Ok(())
    }

    fn fsync(&self, _req: RequestInfo, path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        debug!("fsync() called with {:?}", path);
        let path = path.to_str().unwrap();
        self.client.fsync(path).map_err(into_fuse_error)
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        debug!("opendir() called on {:?}", path);
        Ok((0, 0))
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
        debug!("readdir() called with {:?}", path);
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        return self.client.readdir(inode).map_err(into_fuse_error);
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir() called on {:?}", path);
        Ok(())
    }

    fn fsyncdir(&self, _req: RequestInfo, path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        debug!("fsyncdir() called with {:?}", path);
        let path = path.to_str().unwrap();
        self.client.fsync(path).map_err(into_fuse_error)
    }

    fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
        warn!("statfs() implementation is a stub");
        // TODO: real implementation of this
        Ok(Statfs {
            blocks: 10,
            bfree: 10,
            bavail: 10,
            files: 1,
            ffree: 10,
            bsize: 4096,
            namelen: 128,
            frsize: 4096,
        })
    }

    fn setxattr(
        &self,
        _req: RequestInfo,
        path: &Path,
        name: &OsStr,
        value: &[u8],
        _flags: u32,
        _position: u32,
    ) -> ResultEmpty {
        debug!("setxattr() called with {:?} {:?} {:?}", path, name, value);
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        self.client
            .setxattr(inode, name.to_str().unwrap(), value)
            .map_err(into_fuse_error)
    }

    fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
        debug!("getxattr() called with {:?} {:?}", path, name);
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        if size == 0 {
            self.client
                .getxattr(inode, name.to_str().unwrap())
                .map(|data| Xattr::Size(data.len() as u32))
                .map_err(into_fuse_error)
        } else {
            self.client
                .getxattr(inode, name.to_str().unwrap())
                .map(Xattr::Data)
                .map_err(into_fuse_error)
        }
    }

    fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
        debug!("listxattr() called with {:?}", path);
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        let result = self
            .client
            .listxattr(inode)
            .map(|xattrs| {
                let mut bytes = vec![];
                // Convert to concatenated null-terminated strings
                for attr in xattrs {
                    bytes.extend(attr.as_bytes());
                    bytes.push(0);
                }
                bytes
            })
            .map_err(into_fuse_error);

        if size == 0 {
            result.map(|data| Xattr::Size(data.len() as u32))
        } else {
            result.map(Xattr::Data)
        }
    }

    fn removexattr(&self, _req: RequestInfo, path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("removexattr() called with {:?} {:?}", path, name);
        let path = path.to_str().unwrap();
        let inode = self.lookup_path(path)?;
        self.client
            .removexattr(inode, name.to_str().unwrap())
            .map_err(into_fuse_error)
    }

    fn access(&self, req: RequestInfo, path: &Path, mask: u32) -> ResultEmpty {
        debug!("access() called with {:?} {:?}", path, mask);
        let path = path.to_str().unwrap();
        // TODO: use getgrouplist() to look up all the groups for this user
        self.client
            .access(path, req.uid, &[req.gid], mask)
            .map_err(into_fuse_error)
    }

    fn create(
        &self,
        req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> ResultCreate {
        debug!("create() called with {:?} {:?}", parent, name);
        // TODO: kind of a hack to create the file
        let path = Path::new(parent).join(name);
        self.client
            .write(path.to_str().unwrap(), &[], 0)
            .map_err(into_fuse_error)?;
        let inode = self.lookup_path(path.to_str().unwrap())?;
        self.client
            .chown(inode, Some(req.uid), Some(req.gid))
            .map_err(into_fuse_error)?;
        self.client
            .chmod(path.to_str().unwrap(), mode)
            .map_err(into_fuse_error)?;
        let inode = self.lookup_path(path.to_str().unwrap())?;
        self.client
            .getattr(inode)
            .map(|attr| CreatedEntry {
                ttl: Timespec { sec: 0, nsec: 0 },
                attr,
                fh: 0,
                flags: 0,
            })
            .map_err(into_fuse_error)
    }
}
