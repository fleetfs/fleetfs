use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::Path;

use libc;
use log::debug;
use log::warn;
use time::Timespec;

use crate::client::NodeClient;
use crate::generated::{ErrorCode, FileKind, Timestamp, UserContext};
use crate::storage::metadata_storage::MAX_NAME_LENGTH;
use crate::utils::check_access;
use fuse::{
    Filesystem, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request,
};
use libc::ENOSYS;
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
        ErrorCode::NameTooLong => libc::ENAMETOOLONG,
        ErrorCode::NotEmpty => libc::ENOTEMPTY,
        ErrorCode::AlreadyExists => libc::EEXIST,
        ErrorCode::DefaultValueNotAnError => unreachable!(),
    }
}

fn as_file_kind(mode: u32) -> FileKind {
    if mode & libc::S_IFREG != 0 {
        return FileKind::File;
    } else if mode & libc::S_IFLNK != 0 {
        return FileKind::Symlink;
    } else if mode & libc::S_IFDIR != 0 {
        return FileKind::Directory;
    } else {
        unimplemented!();
    }
}

impl Filesystem for FleetFUSE {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {}

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        // TODO: avoid this double lookup
        match self
            .client
            .lookup(parent, name.to_str().unwrap(), req.uid(), req.gid())
        {
            Ok(inode) => match self.client.getattr(inode) {
                Ok(attr) => reply.entry(&Timespec { sec: 0, nsec: 0 }, &attr, 0),
                Err(error_code) => reply.error(into_fuse_error(error_code)),
            },
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &Request, inode: u64, reply: ReplyAttr) {
        debug!("getattr() called with {:?}", inode);
        match self.client.getattr(inode) {
            Ok(attr) => reply.attr(&Timespec { sec: 0, nsec: 0 }, &attr),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn setattr(
        &mut self,
        req: &Request,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if let Some(mode) = mode {
            debug!("chmod() called with {:?}, {:o}", inode, mode);
            if let Err(error_code) =
                self.client
                    .chmod(inode, mode, UserContext::new(req.uid(), req.gid()))
            {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        if uid.is_some() || gid.is_some() {
            debug!("chown() called with {:?} {:?} {:?}", inode, uid, gid);
            if let Err(error_code) =
                self.client
                    .chown(inode, uid, gid, UserContext::new(req.uid(), req.gid()))
            {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        if let Some(size) = size {
            debug!("truncate() called with {:?}", inode);
            if let Err(error_code) = self.client.truncate(inode, size, req.uid(), req.gid()) {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        if atime.is_some() || mtime.is_some() {
            debug!(
                "utimens() called with {:?}, {:?}, {:?}",
                inode, atime, mtime
            );
            if let Err(error_code) = self.client.utimens(
                inode,
                req.uid(),
                atime.map(|x| Timestamp::new(x.sec, x.nsec)),
                mtime.map(|x| Timestamp::new(x.sec, x.nsec)),
            ) {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        match self.client.getattr(inode) {
            Ok(attr) => reply.attr(&Timespec { sec: 0, nsec: 0 }, &attr),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn readlink(&mut self, req: &Request, inode: u64, reply: ReplyData) {
        debug!("readlink() called on {:?}", inode);
        match self
            .client
            .readlink(inode, UserContext::new(req.uid(), req.gid()))
        {
            Ok(data) => reply.data(&data),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn mknod(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if (mode & (libc::S_IFREG | libc::S_IFLNK)) == 0 {
            // TODO
            warn!("mknod() implementation is incomplete. Only supports regular files and symlinks");
            reply.error(libc::ENOSYS);
        } else {
            match self.client.create(
                parent,
                name.to_str().unwrap(),
                req.uid(),
                req.gid(),
                mode as u16,
                as_file_kind(mode),
            ) {
                Ok(attr) => reply.entry(&Timespec { sec: 0, nsec: 0 }, &attr, 0),
                Err(error_code) => reply.error(into_fuse_error(error_code)),
            }
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        debug!("mkdir() called with {:?} {:?} {:o}", parent, name, mode);
        match self.client.mkdir(
            parent,
            name.to_str().unwrap(),
            req.uid(),
            req.gid(),
            mode as u16,
        ) {
            Ok(attr) => reply.entry(&Timespec { sec: 0, nsec: 0 }, &attr, 0),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink() called with {:?} {:?}", parent, name);
        if let Err(error_code) =
            self.client
                .unlink(parent, name.to_str().unwrap(), req.uid(), req.gid())
        {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir() called with {:?} {:?}", parent, name);
        if let Err(error_code) = self.client.rmdir(
            parent,
            name.to_str().unwrap(),
            UserContext::new(req.uid(), req.gid()),
        ) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn symlink(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        debug!("symlink() called with {:?} {:?} {:?}", parent, name, link);
        let name = name.to_str().unwrap();
        // TODO: Error handling
        match self
            .client
            .create(parent, name, req.uid(), req.gid(), 0o755, FileKind::Symlink)
        {
            Ok(attrs) => {
                self.client
                    .truncate(attrs.ino, 0, req.uid(), req.gid())
                    .unwrap();
                self.client
                    .write(
                        attrs.ino,
                        &Vec::from(link.to_str().unwrap().to_string()),
                        0,
                        UserContext::new(req.uid(), req.gid()),
                    )
                    .unwrap();

                reply.entry(&Timespec { sec: 0, nsec: 0 }, &attrs, 0);
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn rename(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEmpty,
    ) {
        if let Err(error_code) = self.client.rename(
            parent,
            name.to_str().unwrap(),
            new_parent,
            new_name.to_str().unwrap(),
            UserContext::new(req.uid(), req.gid()),
        ) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn link(
        &mut self,
        _req: &Request,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        debug!(
            "link() called for {}, {}, {:?}",
            inode, new_parent, new_name
        );
        match self
            .client
            .hardlink(inode, new_parent, new_name.to_str().unwrap())
        {
            Ok(attr) => reply.entry(&Timespec { sec: 0, nsec: 0 }, &attr, 0),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn open(&mut self, _req: &Request, inode: u64, _flags: u32, reply: ReplyOpen) {
        debug!("open() called for {:?}", inode);
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        debug!("read() called on {:?}", inode);
        assert!(offset >= 0);
        self.client.read(
            inode,
            offset as u64,
            size,
            UserContext::new(req.uid(), req.gid()),
            move |result| match result {
                Ok(data) => reply.data(data),
                Err(error_code) => reply.error(into_fuse_error(error_code)),
            },
        );
    }

    fn write(
        &mut self,
        req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        debug!("write() called with {:?}", inode);
        assert!(offset >= 0);
        match self.client.write(
            inode,
            &data,
            offset as u64,
            UserContext::new(req.uid(), req.gid()),
        ) {
            Ok(written) => reply.written(written),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn flush(&mut self, _req: &Request, inode: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        debug!("flush() called on {:?}", inode);
        reply.error(ENOSYS);
    }

    fn release(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release() called on {:?}", inode);
        reply.ok();
    }

    fn fsync(&mut self, _req: &Request, inode: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        debug!("fsync() called with {:?}", inode);
        if let Err(error_code) = self.client.fsync(inode) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn opendir(&mut self, _req: &Request, inode: u64, _flags: u32, reply: ReplyOpen) {
        debug!("opendir() called on {:?}", inode);
        reply.opened(0, 0);
    }

    // TODO: send offset to server and do pagination
    fn readdir(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir() called with {:?}", inode);
        assert!(offset >= 0);
        match self.client.readdir(inode) {
            Ok(entries) => {
                for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
                    let (inode, name, file_type) = entry;

                    let buffer_full: bool =
                        reply.add(*inode, offset + index as i64 + 1, *file_type, name);

                    if buffer_full {
                        break;
                    }
                }

                reply.ok();
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn releasedir(&mut self, _req: &Request, inode: u64, _fh: u64, _flags: u32, reply: ReplyEmpty) {
        debug!("releasedir() called on {:?}", inode);
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        debug!("fsyncdir() called with {:?}", inode);
        if let Err(error_code) = self.client.fsync(inode) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        warn!("statfs() implementation is a stub");
        // TODO: real implementation of this
        reply.statfs(10, 10, 10, 1, 10, 4096, MAX_NAME_LENGTH, 4096);
    }

    fn setxattr(
        &mut self,
        _req: &Request,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        debug!("setxattr() called with {:?} {:?} {:?}", inode, name, value);
        if let Err(error_code) = self.client.setxattr(inode, name.to_str().unwrap(), value) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn getxattr(&mut self, _req: &Request, inode: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
        debug!("getxattr() called with {:?} {:?}", inode, name);
        match self.client.getxattr(inode, name.to_str().unwrap()) {
            Ok(data) => {
                if size == 0 {
                    reply.size(data.len() as u32);
                } else if data.len() <= size as usize {
                    reply.data(&data);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn listxattr(&mut self, _req: &Request, inode: u64, size: u32, reply: ReplyXattr) {
        debug!("listxattr() called with {:?}", inode);
        match self.client.listxattr(inode).map(|xattrs| {
            let mut bytes = vec![];
            // Convert to concatenated null-terminated strings
            for attr in xattrs {
                bytes.extend(attr.as_bytes());
                bytes.push(0);
            }
            bytes
        }) {
            Ok(data) => {
                if size == 0 {
                    reply.size(data.len() as u32);
                } else if data.len() <= size as usize {
                    reply.data(&data);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn removexattr(&mut self, _req: &Request, inode: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("removexattr() called with {:?} {:?}", inode, name);
        if let Err(error_code) = self.client.removexattr(inode, name.to_str().unwrap()) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    // TODO: maybe mount with the "default_permissions" option. Then this wouldn't be called?
    // TODO: use getgrouplist() to look up all the groups for this user
    fn access(&mut self, req: &Request, inode: u64, mask: u32, reply: ReplyEmpty) {
        debug!("access() called with {:?} {:?}", inode, mask);
        match self.client.getattr(inode) {
            Ok(attr) => {
                if check_access(attr.uid, attr.gid, attr.perm, req.uid(), req.gid(), mask) {
                    reply.ok();
                } else {
                    reply.error(libc::EACCES);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) {
        debug!("create() called with {:?} {:?}", parent, name);
        match self.client.create(
            parent,
            name.to_str().unwrap(),
            req.uid(),
            req.gid(),
            mode as u16,
            as_file_kind(mode),
        ) {
            Ok(attr) => {
                // TODO: implement flags
                reply.created(&Timespec { sec: 0, nsec: 0 }, &attr, 0, 0, 0)
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn getlk(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        reply.error(ENOSYS);
    }

    fn setlk(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        _sleep: bool,
        reply: ReplyEmpty,
    ) {
        reply.error(ENOSYS);
    }

    fn bmap(&mut self, _req: &Request, _ino: u64, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        reply.error(ENOSYS);
    }
}
