use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::generated::{ErrorCode, FileKind, Timestamp, UserContext};
use crate::storage::data_storage::BLOCK_SIZE;
use crate::utils::check_access;
use fuse::FUSE_ROOT_ID;
use std::time::SystemTime;

pub const ROOT_INODE: u64 = FUSE_ROOT_ID;
pub const MAX_NAME_LENGTH: u32 = 255;
pub const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

type Inode = u64;
type DirectoryDescriptor = HashMap<String, (Inode, FileKind)>;

#[derive(Clone, Debug)]
pub struct InodeAttributes {
    pub inode: Inode,
    pub size: u64,
    pub last_accessed: Timestamp,
    pub last_modified: Timestamp,
    pub last_metadata_changed: Timestamp,
    pub kind: FileKind,
    // Permissions and special mode bits
    pub mode: u16,
    pub hardlinks: u32,
    pub uid: u32,
    pub gid: u32,
    pub xattrs: HashMap<String, Vec<u8>>,
}

// TODO: add persistence
pub struct MetadataStorage {
    metadata: Mutex<HashMap<Inode, InodeAttributes>>,
    // Directory contents. This is considered metadata rather than data,
    // because it's replicated to all nodes to allow better searching.
    // Maybe we should revisit that design?
    // Maps directory inodes to maps of name -> inode
    directories: Mutex<HashMap<Inode, DirectoryDescriptor>>,
    // Stores mapping of directory inodes to their parent
    directory_parents: Mutex<HashMap<Inode, Inode>>,
    // Raft guarantees that operations are performed in the same order across all nodes
    // which means that all nodes have the same value for this counter
    next_inode: AtomicU64,
}

impl MetadataStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> MetadataStorage {
        let mut directories = HashMap::new();
        directories.insert(ROOT_INODE, HashMap::new());

        let mut parents = HashMap::new();
        parents.insert(ROOT_INODE, ROOT_INODE);

        let mut metadata = HashMap::new();
        metadata.insert(
            ROOT_INODE,
            InodeAttributes {
                inode: ROOT_INODE,
                size: 0,
                last_accessed: now(),
                last_modified: now(),
                last_metadata_changed: now(),
                kind: FileKind::Directory,
                mode: 0o777,
                hardlinks: 2,
                uid: 0,
                gid: 0,
                xattrs: Default::default(),
            },
        );

        MetadataStorage {
            metadata: Mutex::new(metadata),
            directories: Mutex::new(directories),
            directory_parents: Mutex::new(parents),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        }
    }

    pub fn lookup(
        &self,
        parent: Inode,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> Result<Option<Inode>, ErrorCode> {
        if name.len() > MAX_NAME_LENGTH as usize {
            return Err(ErrorCode::NameTooLong);
        }

        let metadata = self.metadata.lock().unwrap();
        let parent_attrs = metadata.get(&parent).unwrap();
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            uid,
            gid,
            libc::X_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        let directories = self.directories.lock().unwrap();
        let maybe_inode = directories
            .get(&parent)
            .unwrap()
            .get(name)
            .map(|(inode, _)| *inode);
        Ok(maybe_inode)
    }

    pub fn read(&self, inode: Inode, context: UserContext) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        if !check_access(
            inode_attrs.uid,
            inode_attrs.gid,
            inode_attrs.mode,
            context.uid(),
            context.gid(),
            libc::R_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        Ok(())
    }

    pub fn get_xattr(&self, inode: Inode, key: &str) -> Option<Vec<u8>> {
        let metadata = self.metadata.lock().unwrap();
        metadata.get(&inode)?.xattrs.get(key).cloned()
    }

    pub fn list_xattrs(&self, inode: Inode) -> Vec<String> {
        let metadata = self.metadata.lock().unwrap();
        metadata
            .get(&inode)
            .map(|x| x.xattrs.keys().cloned().collect())
            .unwrap_or_else(|| vec![])
    }

    pub fn set_xattr(&self, inode: Inode, key: &str, value: &[u8]) {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        inode_attrs.xattrs.insert(key.to_string(), value.to_vec());
        inode_attrs.last_metadata_changed = now();
    }

    pub fn remove_xattr(&self, inode: Inode, key: &str) {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        inode_attrs.xattrs.remove(key);
        inode_attrs.last_metadata_changed = now();
    }

    pub fn readdir(&self, inode: Inode) -> Result<Vec<(Inode, String, FileKind)>, ErrorCode> {
        let directories = self.directories.lock().unwrap();
        let parents = self.directory_parents.lock().unwrap();

        if let Some(entries) = directories.get(&inode) {
            let parent_inode = *parents.get(&inode).unwrap();
            let mut result: Vec<(Inode, String, FileKind)> = entries
                .iter()
                .map(|(name, (inode, kind))| (*inode, name.clone(), *kind))
                .collect();
            // TODO: kind of a hack
            result.insert(0, (parent_inode, "..".to_string(), FileKind::Directory));
            result.insert(0, (inode, ".".to_string(), FileKind::Directory));
            Ok(result)
        } else {
            Err(ErrorCode::DoesNotExist)
        }
    }

    pub fn utimens(
        &self,
        inode: Inode,
        atime: Option<&Timestamp>,
        mtime: Option<&Timestamp>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();

        let inode_attrs = metadata.get(&inode).unwrap();
        if inode_attrs.uid != context.uid()
            && !check_access(
                inode_attrs.uid,
                inode_attrs.gid,
                inode_attrs.mode,
                context.uid(),
                context.gid(),
                libc::W_OK as u32,
            )
        {
            return Err(ErrorCode::AccessDenied);
        }

        if let Some(inode_metadata) = metadata.get_mut(&inode) {
            // Non-owners are only allowed to change atime & mtime to current:
            // http://man7.org/linux/man-pages/man2/utimensat.2.html
            if inode_metadata.uid != context.uid()
                && context.uid() != 0
                && (atime.map_or(libc::UTIME_NOW as i32, Timestamp::nanos)
                    != libc::UTIME_NOW as i32
                    || mtime.map_or(libc::UTIME_NOW as i32, Timestamp::nanos)
                        != libc::UTIME_NOW as i32)
            {
                return Err(ErrorCode::OperationNotPermitted);
            }

            if let Some(atime) = atime {
                inode_metadata.last_accessed = *atime;
            }
            if let Some(mtime) = mtime {
                inode_metadata.last_modified = *mtime;
            }

            Ok(())
        } else {
            Err(ErrorCode::DoesNotExist)
        }
    }

    pub fn chmod(
        &self,
        inode: Inode,
        mut mode: u32,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        if context.uid() != 0 && inode_attrs.uid != context.uid() {
            return Err(ErrorCode::OperationNotPermitted);
        }

        // TODO: suid/sgid not supported
        mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
        inode_attrs.mode = mode as u16;
        inode_attrs.last_metadata_changed = now();

        Ok(())
    }

    pub fn chown(
        &self,
        inode: Inode,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_metadata = metadata.get_mut(&inode).unwrap();

        // Only root can change uid
        if let Some(uid) = uid {
            if context.uid() != 0
                // but no-op changes by the owner are not an error
                && !(uid == inode_metadata.uid && context.uid() == inode_metadata.uid)
            {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
        // Only owner may change the group
        if gid.is_some() && context.uid() != 0 && context.uid() != inode_metadata.uid {
            return Err(ErrorCode::OperationNotPermitted);
        }

        if let Some(uid) = uid {
            inode_metadata.uid = uid;
        }
        if let Some(gid) = gid {
            inode_metadata.gid = gid;
        }
        if uid.is_some() || gid.is_some() {
            inode_metadata.last_metadata_changed = now();
        }

        Ok(())
    }

    pub fn hardlink(
        &self,
        inode: Inode,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();
        let new_parent_attrs = metadata.get_mut(&new_parent).unwrap();
        if !check_access(
            new_parent_attrs.uid,
            new_parent_attrs.gid,
            new_parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }
        new_parent_attrs.last_modified = now();
        new_parent_attrs.last_metadata_changed = now();

        let inode_attrs = metadata.get_mut(&inode).unwrap();
        inode_attrs.hardlinks += 1;
        inode_attrs.last_metadata_changed = now();

        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_name.to_string(), (inode, inode_attrs.kind));

        Ok(())
    }

    pub fn mkdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();
        let parent_attrs = metadata.get(&parent).unwrap();
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            uid,
            gid,
            libc::W_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        let inode = self.allocate_inode();
        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&parent)
            .unwrap()
            .insert(name.to_string(), (inode, FileKind::Directory));
        directories.insert(inode, HashMap::new());

        let mut parents = self.directory_parents.lock().unwrap();
        parents.insert(inode, parent);

        let inode_metadata = InodeAttributes {
            inode,
            size: BLOCK_SIZE,
            last_accessed: now(),
            last_modified: now(),
            last_metadata_changed: now(),
            kind: FileKind::Directory,
            // TODO: suid/sgid not supported
            mode: mode & !(libc::S_ISUID | libc::S_ISGID) as u16,
            hardlinks: 2,
            uid,
            gid,
            xattrs: Default::default(),
        };
        metadata.insert(inode, inode_metadata);
        metadata.get_mut(&parent).unwrap().last_metadata_changed = now();
        metadata.get_mut(&parent).unwrap().last_modified = now();

        Ok(())
    }

    pub fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut directories = self.directories.lock().unwrap();
        let mut metadata = self.metadata.lock().unwrap();
        if let Some((inode, _)) = directories.get(&parent).unwrap().get(name) {
            let parent_attrs = metadata.get(&parent).unwrap();
            if !check_access(
                parent_attrs.uid,
                parent_attrs.gid,
                parent_attrs.mode,
                context.uid(),
                context.gid(),
                libc::W_OK as u32,
            ) {
                return Err(ErrorCode::AccessDenied);
            }

            // "Sticky bit" handling
            if parent_attrs.mode & libc::S_ISVTX as u16 != 0 {
                let inode_attrs = metadata.get(inode).unwrap();
                if context.uid() != 0
                    && context.uid() != parent_attrs.uid
                    && context.uid() != inode_attrs.uid
                {
                    return Err(ErrorCode::AccessDenied);
                }
            }

            let new_parent_attrs = metadata.get(&new_parent).unwrap();
            if !check_access(
                new_parent_attrs.uid,
                new_parent_attrs.gid,
                new_parent_attrs.mode,
                context.uid(),
                context.gid(),
                libc::W_OK as u32,
            ) {
                return Err(ErrorCode::AccessDenied);
            }

            // "Sticky bit" handling in new_parent
            if new_parent_attrs.mode & libc::S_ISVTX as u16 != 0 {
                if let Some((new_inode, _)) = directories.get(&new_parent).unwrap().get(new_name) {
                    let new_inode_attrs = metadata.get(new_inode).unwrap();
                    if context.uid() != 0
                        && context.uid() != new_parent_attrs.uid
                        && context.uid() != new_inode_attrs.uid
                    {
                        return Err(ErrorCode::AccessDenied);
                    }
                }
            }

            // Only overwrite an existing directory if it's empty
            if let Some((new_inode, _)) = directories.get(&new_parent).unwrap().get(new_name) {
                let new_inode_attrs = metadata.get(new_inode).unwrap();
                if new_inode_attrs.kind == FileKind::Directory
                    && !directories.get(&new_inode).unwrap().is_empty()
                {
                    return Err(ErrorCode::NotEmpty);
                }
            }

            // Only move an existing directory to a new parent, if we have write access to it,
            // because that will change the ".." link in it
            let inode_attrs = metadata.get(inode).unwrap();
            if inode_attrs.kind == FileKind::Directory
                && parent != new_parent
                && !check_access(
                    inode_attrs.uid,
                    inode_attrs.gid,
                    inode_attrs.mode,
                    context.uid(),
                    context.gid(),
                    libc::W_OK as u32,
                )
            {
                return Err(ErrorCode::AccessDenied);
            }
        }

        let entry = directories.get_mut(&parent).unwrap().remove(name).unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_name.to_string(), entry);

        let (inode, kind) = entry;
        if kind == FileKind::Directory {
            let mut parents = self.directory_parents.lock().unwrap();
            parents.insert(inode, new_parent);
        }

        metadata.get_mut(&parent).unwrap().last_metadata_changed = now();
        metadata.get_mut(&parent).unwrap().last_modified = now();
        metadata.get_mut(&new_parent).unwrap().last_metadata_changed = now();
        metadata.get_mut(&new_parent).unwrap().last_modified = now();
        metadata.get_mut(&inode).unwrap().last_metadata_changed = now();

        Ok(())
    }

    // TODO: should have some error handling
    pub fn truncate(
        &self,
        inode: Inode,
        new_length: u64,
        uid: u32,
        gid: u32,
    ) -> Result<(), ErrorCode> {
        if new_length > MAX_FILE_SIZE {
            return Err(ErrorCode::FileTooLarge);
        }

        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        if !check_access(
            inode_attrs.uid,
            inode_attrs.gid,
            inode_attrs.mode,
            uid,
            gid,
            libc::W_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        inode_attrs.size = new_length;
        inode_attrs.last_metadata_changed = now();
        inode_attrs.last_modified = now();

        Ok(())
    }

    // Returns an inode, if that inode's data should be deleted
    pub fn unlink(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> Result<Option<Inode>, ErrorCode> {
        let mut directories = self.directories.lock().unwrap();
        let parent_directory = directories.get_mut(&parent).unwrap();
        let (inode, _) = parent_directory.get(name).unwrap();

        let mut metadata = self.metadata.lock().unwrap();

        let parent_attrs = metadata.get(&parent).unwrap();
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            uid,
            gid,
            libc::W_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        // "Sticky bit" handling
        if parent_attrs.mode & libc::S_ISVTX as u16 != 0 {
            let inode_attrs = metadata.get(inode).unwrap();
            if uid != 0 && uid != parent_attrs.uid && uid != inode_attrs.uid {
                return Err(ErrorCode::AccessDenied);
            }
        }

        let parent_attrs = metadata.get_mut(&parent).unwrap();
        parent_attrs.last_metadata_changed = now();
        parent_attrs.last_modified = now();
        let (inode, _) = parent_directory.remove(name).unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        inode_attrs.hardlinks -= 1;
        inode_attrs.last_metadata_changed = now();
        if inode_attrs.hardlinks == 0 {
            metadata.remove(&inode);
            return Ok(Some(inode));
        }

        Ok(None)
    }

    pub fn rmdir(&self, parent: u64, name: &str, context: UserContext) -> Result<(), ErrorCode> {
        let mut directories = self.directories.lock().unwrap();
        let mut metadata = self.metadata.lock().unwrap();
        if let Some((inode, _)) = directories.get(&parent).unwrap().get(name) {
            if !directories
                .get(&inode)
                .map(HashMap::is_empty)
                .unwrap_or(true)
            {
                return Err(ErrorCode::NotEmpty);
            }
            let parent_attrs = metadata.get(&parent).unwrap();
            if !check_access(
                parent_attrs.uid,
                parent_attrs.gid,
                parent_attrs.mode,
                context.uid(),
                context.gid(),
                libc::W_OK as u32,
            ) {
                return Err(ErrorCode::AccessDenied);
            }

            // "Sticky bit" handling
            if parent_attrs.mode & libc::S_ISVTX as u16 != 0 {
                let inode_attrs = metadata.get(inode).unwrap();
                if context.uid() != 0
                    && context.uid() != parent_attrs.uid
                    && context.uid() != inode_attrs.uid
                {
                    return Err(ErrorCode::AccessDenied);
                }
            }
        }

        if let Some((inode, _)) = directories.get_mut(&parent).unwrap().remove(name) {
            directories.remove(&inode);
            metadata.remove(&inode);
            metadata.get_mut(&parent).unwrap().last_metadata_changed = now();
            metadata.get_mut(&parent).unwrap().last_modified = now();
            let mut parents = self.directory_parents.lock().unwrap();
            parents.remove(&inode);
        }

        Ok(())
    }

    pub fn write(
        &self,
        inode: Inode,
        offset: u64,
        length: u32,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_metadata = metadata.get_mut(&inode).unwrap();
        if !check_access(
            inode_metadata.uid,
            inode_metadata.gid,
            inode_metadata.mode,
            context.uid(),
            context.gid(),
            libc::W_OK as u32,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        let current_length = inode_metadata.size;
        inode_metadata.size = max(current_length, u64::from(length) + offset);
        inode_metadata.last_metadata_changed = now();
        inode_metadata.last_modified = now();

        Ok(())
    }

    pub fn create(
        &self,
        parent: Inode,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    ) -> Result<(Inode, InodeAttributes), ErrorCode> {
        if self.lookup(parent, name, uid, gid)?.is_none() {
            let mut metadata = self.metadata.lock().unwrap();
            let parent_attrs = metadata.get(&parent).unwrap();
            if !check_access(
                parent_attrs.uid,
                parent_attrs.gid,
                parent_attrs.mode,
                uid,
                gid,
                libc::W_OK as u32,
            ) {
                return Err(ErrorCode::AccessDenied);
            }

            let inode = self.allocate_inode();
            let mut directories = self.directories.lock().unwrap();
            directories
                .get_mut(&parent)
                .unwrap()
                .insert(name.to_string(), (inode, FileKind::File));

            let inode_metadata = InodeAttributes {
                inode,
                size: 0,
                last_accessed: now(),
                last_modified: now(),
                last_metadata_changed: now(),
                kind,
                // TODO: suid/sgid not supported
                mode: mode & !(libc::S_ISUID | libc::S_ISGID) as u16,
                hardlinks: 1,
                uid,
                gid,
                xattrs: Default::default(),
            };
            metadata.insert(inode, inode_metadata.clone());
            metadata.get_mut(&parent).unwrap().last_metadata_changed = now();
            metadata.get_mut(&parent).unwrap().last_modified = now();
            Ok((inode, inode_metadata))
        } else {
            Err(ErrorCode::AlreadyExists)
        }
    }

    pub fn get_attributes(&self, inode: Inode) -> Option<InodeAttributes> {
        // TODO: find a way to avoid this clone()
        self.metadata.lock().unwrap().get(&inode).cloned()
    }

    fn allocate_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }
}

fn now() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time before unix epoch");
    Timestamp::new(now.as_secs() as i64, now.subsec_nanos() as i32)
}
