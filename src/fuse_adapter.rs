use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use log::debug;
use log::error;
use log::warn;

use crate::ErrorCode;
use crate::base::check_access;
use crate::base::{FileKind, Timestamp, UserContext};
use crate::client::NodeClient;
use fuser::{
    BsdFileFlags, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, KernelConfig,
    LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const FMODE_EXEC: i32 = 0x20;

struct FileHandleAttributes {
    read: bool,
    write: bool,
}

pub struct FleetFUSE {
    client: NodeClient,
    next_file_handle: AtomicU64,
    direct_io: bool,
    file_handles: Mutex<HashMap<u64, FileHandleAttributes>>,
}

impl FleetFUSE {
    pub fn new(server_ip_port: SocketAddr, direct_io: bool) -> FleetFUSE {
        FleetFUSE {
            client: NodeClient::new(server_ip_port),
            next_file_handle: AtomicU64::new(1),
            direct_io,
            file_handles: Mutex::new(HashMap::new()),
        }
    }

    fn allocate_file_handle(&self, read: bool, write: bool) -> u64 {
        let handle = self.next_file_handle.fetch_add(1, Ordering::SeqCst);
        let mut handles = self
            .file_handles
            .lock()
            .expect("file_handles lock is poisoned");
        handles.insert(handle, FileHandleAttributes { read, write });

        handle
    }

    fn deallocate_file_handle(&self, handle: u64) {
        let mut handles = self
            .file_handles
            .lock()
            .expect("file_handles lock is poisoned");
        handles.remove(&handle);
    }

    fn check_read(&self, handle: u64) -> bool {
        let handles = self
            .file_handles
            .lock()
            .expect("file_handles lock is poisoned");
        if let Some(value) = handles.get(&handle).map(|x| x.read) {
            value
        } else {
            error!("Undefined file handle: {}", handle);
            false
        }
    }

    fn check_write(&self, handle: u64) -> bool {
        let handles = self
            .file_handles
            .lock()
            .expect("file_handles lock is poisoned");
        if let Some(value) = handles.get(&handle).map(|x| x.write) {
            value
        } else {
            error!("Undefined file handle: {}", handle);
            false
        }
    }
}

fn into_fuse_error(error: ErrorCode) -> Errno {
    match error {
        ErrorCode::DoesNotExist => Errno::ENOENT,
        ErrorCode::InodeDoesNotExist => Errno::EBADF,
        ErrorCode::Uncategorized => Errno::EIO,
        ErrorCode::Corrupted => Errno::EIO,
        ErrorCode::RaftFailure => Errno::EIO,
        ErrorCode::BadResponse => Errno::EIO,
        ErrorCode::BadRequest => Errno::EIO,
        ErrorCode::FileTooLarge => Errno::EFBIG,
        ErrorCode::AccessDenied => Errno::EACCES,
        ErrorCode::OperationNotPermitted => Errno::EPERM,
        ErrorCode::NameTooLong => Errno::ENAMETOOLONG,
        ErrorCode::NotEmpty => Errno::ENOTEMPTY,
        ErrorCode::MissingXattrKey => Errno::NO_XATTR,
        ErrorCode::AlreadyExists => Errno::EEXIST,
        ErrorCode::InvalidXattrNamespace => Errno::ENOTSUP,
    }
}

// t_mode type is u16 on MacOS, but u32 on Linux
#[allow(clippy::unnecessary_cast)]
fn as_file_kind(mut mode: u32) -> FileKind {
    mode &= libc::S_IFMT as u32;

    if mode == libc::S_IFREG as u32 {
        FileKind::File
    } else if mode == libc::S_IFLNK as u32 {
        FileKind::Symlink
    } else if mode == libc::S_IFDIR as u32 {
        FileKind::Directory
    } else {
        unimplemented!("{}", mode);
    }
}

impl Filesystem for FleetFUSE {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        Ok(())
    }

    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        // TODO: avoid this double lookup
        match self
            .client
            .lookup(parent.0, name, UserContext::new(req.uid(), req.gid()))
        {
            Ok(inode) => match self.client.getattr(inode) {
                Ok(attr) => reply.entry(&Duration::new(0, 0), &attr, Generation(0)),
                Err(error_code) => reply.error(into_fuse_error(error_code)),
            },
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn forget(&self, _req: &Request, _ino: INodeNo, _nlookup: u64) {}

    fn getattr(&self, _req: &Request, inode: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        debug!("getattr() called with {:?}", inode);
        match self.client.getattr(inode.0) {
            Ok(attr) => reply.attr(&Duration::new(0, 0), &attr),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn setattr(
        &self,
        req: &Request,
        inode: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        if let Some(mode) = mode {
            debug!("chmod() called with {:?}, {:o}", inode, mode);
            if let Err(error_code) =
                self.client
                    .chmod(inode.0, mode, UserContext::new(req.uid(), req.gid()))
            {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        if uid.is_some() || gid.is_some() {
            debug!("chown() called with {:?} {:?} {:?}", inode, uid, gid);
            if let Some(gid) = gid {
                // Non-root users can only change gid to a group they're in
                if req.uid() != 0 && !get_groups(req.pid()).contains(&gid) {
                    reply.error(Errno::EPERM);
                    return;
                }
            }
            if let Err(error_code) =
                self.client
                    .chown(inode.0, uid, gid, UserContext::new(req.uid(), req.gid()))
            {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        if let Some(size) = size {
            debug!("truncate() called with {:?}", inode);
            if let Some(handle) = fh {
                // If the file handle is available, check access locally.
                // This is important as it preserves the semantic that a file handle opened
                // with W_OK will never fail to truncate, even if the file has been subsequently
                // chmod'ed
                if self.check_write(handle.0) {
                    if let Err(error_code) =
                        self.client.truncate(inode.0, size, UserContext::new(0, 0))
                    {
                        reply.error(into_fuse_error(error_code));
                        return;
                    }
                } else {
                    reply.error(Errno::EACCES);
                    return;
                }
            } else if let Err(error_code) =
                self.client
                    .truncate(inode.0, size, UserContext::new(req.uid(), req.gid()))
            {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        if atime.is_some() || mtime.is_some() {
            debug!(
                "utimens() called with {:?}, {:?}, {:?}",
                inode, atime, mtime
            );
            let atimestamp = atime.map(|time_or_now| match time_or_now {
                TimeOrNow::SpecificTime(x) => {
                    let (seconds, nanos) = match x.duration_since(UNIX_EPOCH) {
                        Ok(x) => (x.as_secs() as i64, x.subsec_nanos() as i32),
                        // We get an error if the timestamp if before the epoch
                        Err(error) => (
                            -(error.duration().as_secs() as i64),
                            error.duration().subsec_nanos() as i32,
                        ),
                    };
                    Timestamp::new(seconds, nanos)
                }
                TimeOrNow::Now => Timestamp::new(0, libc::UTIME_NOW as i32),
            });
            let mtimestamp = mtime.map(|time_or_now| match time_or_now {
                TimeOrNow::SpecificTime(x) => {
                    let (seconds, nanos) = match x.duration_since(UNIX_EPOCH) {
                        Ok(x) => (x.as_secs() as i64, x.subsec_nanos() as i32),
                        // We get an error if the timestamp if before the epoch
                        Err(error) => (
                            -(error.duration().as_secs() as i64),
                            error.duration().subsec_nanos() as i32,
                        ),
                    };
                    Timestamp::new(seconds, nanos)
                }
                TimeOrNow::Now => Timestamp::new(0, libc::UTIME_NOW as i32),
            });
            if let Err(error_code) = self.client.utimens(
                inode.0,
                atimestamp,
                mtimestamp,
                UserContext::new(req.uid(), req.gid()),
            ) {
                reply.error(into_fuse_error(error_code));
                return;
            }
        }

        match self.client.getattr(inode.0) {
            Ok(attr) => reply.attr(&Duration::new(0, 0), &attr),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn readlink(&self, _req: &Request, inode: INodeNo, reply: ReplyData) {
        debug!("readlink() called on {:?}", inode);
        match self.client.readlink(inode.0) {
            Ok(data) => reply.data(&data),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    // t_mode type is u16 on MacOS, but u32 on Linux
    #[allow(clippy::unnecessary_cast)]
    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        let file_type = mode & libc::S_IFMT as u32;

        if file_type != libc::S_IFREG as u32
            && file_type != libc::S_IFLNK as u32
            && file_type != libc::S_IFDIR as u32
        {
            // TODO
            warn!(
                "mknod() implementation is incomplete. Only supports regular files, symlinks, and directories. Got {:o}",
                mode
            );
            reply.error(Errno::ENOSYS);
        } else {
            match self.client.create(
                parent.0,
                name,
                req.uid(),
                req.gid(),
                mode as u16,
                as_file_kind(mode),
            ) {
                Ok(attr) => reply.entry(&Duration::new(0, 0), &attr, Generation(0)),
                Err(error_code) => reply.error(into_fuse_error(error_code)),
            }
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        debug!("mkdir() called with {:?} {:?} {:o}", parent, name, mode);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        match self
            .client
            .mkdir(parent.0, name, req.uid(), req.gid(), mode as u16)
        {
            Ok(attr) => reply.entry(&Duration::new(0, 0), &attr, Generation(0)),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink() called with {:?} {:?}", parent, name);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        if let Err(error_code) =
            self.client
                .unlink(parent.0, name, UserContext::new(req.uid(), req.gid()))
        {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir() called with {:?} {:?}", parent, name);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        if let Err(error_code) =
            self.client
                .rmdir(parent.0, name, UserContext::new(req.uid(), req.gid()))
        {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        debug!("symlink() called with {:?} {:?} {:?}", parent, name, link);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        let link = if let Some(value) = link.to_str() {
            value
        } else {
            error!("Link is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };

        match self.client.create(
            parent.0,
            name,
            req.uid(),
            req.gid(),
            0o755,
            FileKind::Symlink,
        ) {
            Ok(attrs) => {
                if let Err(error_code) =
                    self.client
                        .truncate(attrs.ino.0, 0, UserContext::new(req.uid(), req.gid()))
                {
                    reply.error(into_fuse_error(error_code));
                    return;
                }
                if let Err(error_code) =
                    self.client
                        .write(attrs.ino.0, &Vec::from(link.to_string()), 0)
                {
                    reply.error(into_fuse_error(error_code));
                    return;
                }

                reply.entry(&Duration::new(0, 0), &attrs, Generation(0));
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        new_parent: INodeNo,
        new_name: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        let new_name = if let Some(value) = new_name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        if let Err(error_code) = self.client.rename(
            parent.0,
            name,
            new_parent.0,
            new_name,
            UserContext::new(req.uid(), req.gid()),
        ) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn link(
        &self,
        req: &Request,
        inode: INodeNo,
        new_parent: INodeNo,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        debug!(
            "link() called for {}, {}, {:?}",
            inode, new_parent, new_name
        );
        let new_name = if let Some(value) = new_name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        match self.client.hardlink(
            inode.0,
            new_parent.0,
            new_name,
            UserContext::new(req.uid(), req.gid()),
        ) {
            Ok(attr) => reply.entry(&Duration::new(0, 0), &attr, Generation(0)),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn open(&self, req: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        debug!("open() called for {:?}", inode);
        let (access_mask, read, write) = match flags.acc_mode() {
            fuser::OpenAccMode::O_RDONLY => {
                // Behavior is undefined, but most filesystems return EACCES
                if flags.0 & libc::O_TRUNC != 0 {
                    reply.error(Errno::EACCES);
                    return;
                }
                if flags.0 & FMODE_EXEC != 0 {
                    // Open is from internal exec syscall
                    (libc::X_OK, true, false)
                } else {
                    (libc::R_OK, true, false)
                }
            }
            fuser::OpenAccMode::O_WRONLY => (libc::W_OK, false, true),
            fuser::OpenAccMode::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
        };

        match self.client.getattr(inode.0) {
            Ok(attr) => {
                if check_access(
                    attr.uid,
                    attr.gid,
                    attr.perm,
                    req.uid(),
                    req.gid(),
                    access_mask,
                ) {
                    let flags = if self.direct_io {
                        FopenFlags::FOPEN_DIRECT_IO
                    } else {
                        FopenFlags::empty()
                    };
                    reply.opened(FileHandle(self.allocate_file_handle(read, write)), flags);
                } else {
                    reply.error(Errno::EACCES);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn read(
        &self,
        _req: &Request,
        inode: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        debug!("read() called on {:?}", inode);
        if !self.check_read(fh.0) {
            reply.error(Errno::EACCES);
            return;
        }

        self.client
            .read(inode.0, offset, size, move |result| match result {
                Ok(data) => reply.data(data),
                Err(error_code) => reply.error(into_fuse_error(error_code)),
            });
    }

    fn write(
        &self,
        _req: &Request,
        inode: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        debug!("write() called with {:?}", inode);
        if !self.check_write(fh.0) {
            reply.error(Errno::EACCES);
            return;
        }
        match self.client.write(inode.0, data, offset) {
            Ok(written) => reply.written(written),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        inode: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        debug!("flush() called on {:?}", inode);
        reply.error(Errno::ENOSYS);
    }

    fn release(
        &self,
        _req: &Request,
        inode: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release() called on {:?} {}", inode, fh);
        self.deallocate_file_handle(fh.0);
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        inode: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        debug!("fsync() called with {:?}", inode);
        if let Err(error_code) = self.client.fsync(inode.0) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn opendir(&self, req: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        debug!("opendir() called on {:?}", inode);
        let (access_mask, read, write) = match flags.acc_mode() {
            fuser::OpenAccMode::O_RDONLY => {
                // Behavior is undefined, but most filesystems return EACCES
                if flags.0 & libc::O_TRUNC != 0 {
                    reply.error(Errno::EACCES);
                    return;
                }
                (libc::R_OK, true, false)
            }
            fuser::OpenAccMode::O_WRONLY => (libc::W_OK, false, true),
            fuser::OpenAccMode::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
        };

        match self.client.getattr(inode.0) {
            Ok(attr) => {
                if check_access(
                    attr.uid,
                    attr.gid,
                    attr.perm,
                    req.uid(),
                    req.gid(),
                    access_mask,
                ) {
                    let flags = if self.direct_io {
                        FopenFlags::FOPEN_DIRECT_IO
                    } else {
                        FopenFlags::empty()
                    };
                    reply.opened(FileHandle(self.allocate_file_handle(read, write)), flags);
                } else {
                    reply.error(Errno::EACCES);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    // TODO: send offset to server and do pagination
    fn readdir(
        &self,
        _req: &Request,
        inode: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir() called with {:?}", inode);
        match self.client.readdir(inode.0) {
            Ok(entries) => {
                for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
                    let (inode, name, file_type) = entry;

                    let buffer_full: bool =
                        reply.add(INodeNo(*inode), offset + index as u64 + 1, *file_type, name);

                    if buffer_full {
                        break;
                    }
                }

                reply.ok();
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn releasedir(
        &self,
        _req: &Request,
        inode: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        debug!("releasedir() called on {:?} {}", inode, fh);
        self.deallocate_file_handle(fh.0);
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        inode: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        debug!("fsyncdir() called with {:?}", inode);
        if let Err(error_code) = self.client.fsync(inode.0) {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        debug!("statfs()");
        match self.client.statfs() {
            Ok(info) => reply.statfs(
                10,
                10,
                10,
                1,
                10,
                info.block_size,
                info.max_name_length,
                4096,
            ),
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn setxattr(
        &self,
        req: &Request,
        inode: INodeNo,
        name: &OsStr,
        value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        debug!("setxattr() called with {:?} {:?} {:?}", inode, name, value);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Key is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        if let Err(error_code) =
            self.client
                .setxattr(inode.0, name, value, UserContext::new(req.uid(), req.gid()))
        {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn getxattr(&self, req: &Request, inode: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        debug!("getxattr() called with {:?} {:?}", inode, name);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Key is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        match self
            .client
            .getxattr(inode.0, name, UserContext::new(req.uid(), req.gid()))
        {
            Ok(data) => {
                if size == 0 {
                    reply.size(data.len() as u32);
                } else if data.len() <= size as usize {
                    reply.data(&data);
                } else {
                    reply.error(Errno::ERANGE);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn listxattr(&self, _req: &Request, inode: INodeNo, size: u32, reply: ReplyXattr) {
        debug!("listxattr() called with {:?}", inode);
        match self.client.listxattr(inode.0).map(|xattrs| {
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
                    reply.error(Errno::ERANGE);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn removexattr(&self, req: &Request, inode: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        debug!("removexattr() called with {:?} {:?}", inode, name);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Key is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        if let Err(error_code) =
            self.client
                .removexattr(inode.0, name, UserContext::new(req.uid(), req.gid()))
        {
            reply.error(into_fuse_error(error_code));
        } else {
            reply.ok();
        }
    }

    fn access(&self, req: &Request, inode: INodeNo, mask: fuser::AccessFlags, reply: ReplyEmpty) {
        debug!("access() called with {:?} {:?}", inode, mask);
        match self.client.getattr(inode.0) {
            Ok(attr) => {
                if check_access(
                    attr.uid,
                    attr.gid,
                    attr.perm,
                    req.uid(),
                    req.gid(),
                    mask.bits() as i32,
                ) {
                    reply.ok();
                } else {
                    reply.error(Errno::EACCES);
                }
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!("create() called with {:?} {:?}", parent, name);
        let name = if let Some(value) = name.to_str() {
            value
        } else {
            error!("Path component is not UTF-8");
            reply.error(Errno::EINVAL);
            return;
        };
        let (read, write) = match OpenFlags(flags).acc_mode() {
            fuser::OpenAccMode::O_RDONLY => (true, false),
            fuser::OpenAccMode::O_WRONLY => (false, true),
            fuser::OpenAccMode::O_RDWR => (true, true),
        };
        match self.client.create(
            parent.0,
            name,
            req.uid(),
            req.gid(),
            mode as u16,
            as_file_kind(mode),
        ) {
            Ok(attr) => {
                let flags = if self.direct_io {
                    FopenFlags::FOPEN_DIRECT_IO
                } else {
                    FopenFlags::empty()
                };
                // TODO: implement flags
                reply.created(
                    &Duration::new(0, 0),
                    &attr,
                    Generation(0),
                    FileHandle(self.allocate_file_handle(read, write)),
                    flags,
                )
            }
            Err(error_code) => reply.error(into_fuse_error(error_code)),
        }
    }

    fn getlk(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn setlk(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        _sleep: bool,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn bmap(&self, _req: &Request, _ino: INodeNo, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        reply.error(Errno::ENOSYS);
    }
}

fn get_groups(pid: u32) -> Vec<u32> {
    let path = format!("/proc/{pid}/task/{pid}/status");
    let file = File::open(path).unwrap();
    for line in BufReader::new(file).lines() {
        let line = line.unwrap();
        if line.starts_with("Groups:") {
            return line["Groups: ".len()..]
                .split(' ')
                .filter(|x| !x.trim().is_empty())
                .map(|x| x.parse::<u32>().unwrap())
                .collect();
        }
    }

    vec![]
}
