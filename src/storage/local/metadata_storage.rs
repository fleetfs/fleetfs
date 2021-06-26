use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::base::check_access;
use crate::generated::{ErrorCode, FileKind, Timestamp, UserContext};
use crate::storage::local::data_storage::BLOCK_SIZE;
use fuser::FUSE_ROOT_ID;
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

impl InodeAttributes {
    pub fn blocks(&self) -> u64 {
        // TODO: seems like this should be rounded up? Is that a bug?
        self.size / BLOCK_SIZE
    }
}

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
enum XattrNamespace {
    SECURITY,
    SYSTEM,
    TRUSTED,
    USER,
}

fn parse_xattr_namespace(key: &str) -> Result<XattrNamespace, ErrorCode> {
    if let Some(namespace) = key.split('.').next() {
        match namespace {
            "security" => Ok(XattrNamespace::SECURITY),
            "system" => Ok(XattrNamespace::SYSTEM),
            "trusted" => Ok(XattrNamespace::TRUSTED),
            "user" => Ok(XattrNamespace::USER),
            _ => Err(ErrorCode::InvalidXattrNamespace),
        }
    } else {
        Err(ErrorCode::InvalidXattrNamespace)
    }
}

fn xattr_access_check(
    key: &str,
    access_mask: i32,
    inode_attrs: &InodeAttributes,
    context: &UserContext,
) -> Result<(), ErrorCode> {
    match parse_xattr_namespace(key)? {
        XattrNamespace::SECURITY => {
            if access_mask != libc::R_OK && context.uid() != 0 {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
        XattrNamespace::TRUSTED => {
            if context.uid() != 0 {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
        XattrNamespace::SYSTEM => match key {
            "system.posix_acl_access" => {
                if !check_access(
                    inode_attrs.uid,
                    inode_attrs.gid,
                    inode_attrs.mode,
                    context.uid(),
                    context.gid(),
                    access_mask,
                ) {
                    return Err(ErrorCode::OperationNotPermitted);
                }
            }
            _ => {
                if context.uid() != 0 {
                    return Err(ErrorCode::OperationNotPermitted);
                }
            }
        },
        XattrNamespace::USER => {
            if !check_access(
                inode_attrs.uid,
                inode_attrs.gid,
                inode_attrs.mode,
                context.uid(),
                context.gid(),
                access_mask,
            ) {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
    }

    Ok(())
}

// TODO: add persistence
// When acquiring locks on multiple fields, they must be in alphabetical order
pub struct MetadataStorage {
    // Directory contents. This is considered metadata rather than data,
    // because it's replicated to all nodes to allow better searching.
    // Maybe we should revisit that design?
    // Maps directory inodes to maps of name -> inode
    directories: Mutex<HashMap<Inode, DirectoryDescriptor>>,
    // Stores mapping of directory inodes to their parent
    directory_parents: Mutex<HashMap<Inode, Inode>>,
    metadata: Mutex<HashMap<Inode, InodeAttributes>>,
    // Raft guarantees that operations are performed in the same order across all nodes
    // which means that all nodes have the same value for this counter
    next_inode: AtomicU64,
    num_raft_groups: u64,
}

impl MetadataStorage {
    #[allow(clippy::new_without_default)]
    pub fn new(raft_group: u16, num_raft_groups: u16) -> MetadataStorage {
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

        // Each raft group is responsible for inodes modulo num_raft_groups
        let mut start_inode = ROOT_INODE + 1;
        while start_inode % num_raft_groups as u64 != raft_group as u64 {
            start_inode += 1;
        }

        MetadataStorage {
            metadata: Mutex::new(metadata),
            directories: Mutex::new(directories),
            directory_parents: Mutex::new(parents),
            next_inode: AtomicU64::new(start_inode),
            num_raft_groups: num_raft_groups as u64,
        }
    }

    pub(super) fn non_directory_inodes(&self) -> Result<Vec<u64>, ErrorCode> {
        let metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut result = vec![];
        for (inode, attrs) in metadata.iter() {
            if attrs.kind != FileKind::Directory {
                result.push(*inode);
            }
        }

        Ok(result)
    }

    pub fn lookup(
        &self,
        parent: Inode,
        name: &str,
        context: UserContext,
    ) -> Result<Option<Inode>, ErrorCode> {
        if name.len() > MAX_NAME_LENGTH as usize {
            return Err(ErrorCode::NameTooLong);
        }

        let directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let parent_attrs = metadata.get(&parent).ok_or(ErrorCode::InodeDoesNotExist)?;
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::X_OK,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        let maybe_inode = directories[&parent].get(name).map(|(inode, _)| *inode);
        Ok(maybe_inode)
    }

    pub fn get_xattr(
        &self,
        inode: Inode,
        key: &str,
        context: UserContext,
    ) -> Result<Vec<u8>, ErrorCode> {
        let metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata.get(&inode).ok_or(ErrorCode::InodeDoesNotExist)?;
        xattr_access_check(key, libc::R_OK, inode_attrs, &context)?;

        inode_attrs
            .xattrs
            .get(key)
            .cloned()
            .ok_or(ErrorCode::MissingXattrKey)
    }

    pub fn list_xattrs(&self, inode: Inode) -> Result<Vec<String>, ErrorCode> {
        let metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        Ok(metadata
            .get(&inode)
            .map(|x| x.xattrs.keys().cloned().collect())
            .unwrap_or_else(Vec::new))
    }

    pub fn set_xattr(
        &self,
        inode: Inode,
        key: &str,
        value: &[u8],
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        xattr_access_check(key, libc::W_OK, inode_attrs, &context)?;
        inode_attrs.xattrs.insert(key.to_string(), value.to_vec());
        inode_attrs.last_metadata_changed = now();

        Ok(())
    }

    pub fn remove_xattr(
        &self,
        inode: Inode,
        key: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        xattr_access_check(key, libc::W_OK, inode_attrs, &context)?;
        if inode_attrs.xattrs.remove(key).is_none() {
            return Err(ErrorCode::MissingXattrKey);
        }
        inode_attrs.last_metadata_changed = now();

        Ok(())
    }

    pub fn readdir(&self, inode: Inode) -> Result<Vec<(Inode, String, FileKind)>, ErrorCode> {
        let directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let parents = self
            .directory_parents
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;

        if let Some(entries) = directories.get(&inode) {
            let parent_inode = *parents.get(&inode).ok_or(ErrorCode::InodeDoesNotExist)?;
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
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;

        let inode_attrs = metadata.get(&inode).ok_or(ErrorCode::InodeDoesNotExist)?;
        // Non-owners are only allowed to change atime & mtime to current:
        // http://man7.org/linux/man-pages/man2/utimensat.2.html
        if inode_attrs.uid != context.uid()
            && context.uid() != 0
            && (!atime.map_or(false, |x| x.nanos() == libc::UTIME_NOW as i32)
                || !mtime.map_or(false, |x| x.nanos() == libc::UTIME_NOW as i32))
        {
            return Err(ErrorCode::OperationNotPermitted);
        }

        if inode_attrs.uid != context.uid()
            && !check_access(
                inode_attrs.uid,
                inode_attrs.gid,
                inode_attrs.mode,
                context.uid(),
                context.gid(),
                libc::W_OK,
            )
        {
            return Err(ErrorCode::AccessDenied);
        }

        if let Some(inode_metadata) = metadata.get_mut(&inode) {
            if let Some(atime) = atime {
                if atime.nanos() == libc::UTIME_NOW as i32 {
                    // TODO: this should be set during proposal. Currently each node set its own timestamp
                    inode_metadata.last_accessed = now();
                } else {
                    inode_metadata.last_accessed = *atime;
                }
            }
            if let Some(mtime) = mtime {
                if mtime.nanos() == libc::UTIME_NOW as i32 {
                    // TODO: this should be set during proposal. Currently each node set its own timestamp
                    inode_metadata.last_modified = now();
                } else {
                    inode_metadata.last_modified = *mtime;
                }
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
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
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
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_metadata = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;

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

    pub fn hardlink_stage0_link_increment(
        &self,
        inode: Inode,
    ) -> Result<(Timestamp, FileKind), ErrorCode> {
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        inode_attrs.hardlinks += 1;
        let changed = inode_attrs.last_metadata_changed;
        inode_attrs.last_metadata_changed = now();

        Ok((changed, inode_attrs.kind))
    }

    pub fn create_link(
        &self,
        inode: Inode,
        parent: u64,
        name: &str,
        context: UserContext,
        inode_kind: FileKind,
    ) -> Result<(), ErrorCode> {
        if self.lookup(parent, name, context)?.is_some() {
            return Err(ErrorCode::AlreadyExists);
        }

        let mut directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let parent_attrs = metadata
            .get_mut(&parent)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK,
        ) {
            return Err(ErrorCode::AccessDenied);
        }
        parent_attrs.last_modified = now();
        parent_attrs.last_metadata_changed = now();

        directories
            .get_mut(&parent)
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .insert(name.to_string(), (inode, inode_kind));

        Ok(())
    }

    pub fn replace_link(
        &self,
        parent: u64,
        name: &str,
        new_inode: Inode,
        inode_kind: FileKind,
        context: UserContext,
    ) -> Result<u64, ErrorCode> {
        let mut directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let parent_attrs = metadata
            .get_mut(&parent)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK,
        ) {
            return Err(ErrorCode::AccessDenied);
        }
        parent_attrs.last_modified = now();
        parent_attrs.last_metadata_changed = now();

        let (old_inode, _) = directories
            .get_mut(&parent)
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .insert(name.to_string(), (new_inode, inode_kind))
            .unwrap();

        Ok(old_inode)
    }

    pub fn hardlink_rollback(
        &self,
        inode: Inode,
        last_metadata_changed: Timestamp,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        inode_attrs.hardlinks -= 1;
        inode_attrs.last_metadata_changed = last_metadata_changed;

        Ok(())
    }

    pub fn update_parent(&self, inode: u64, new_parent: u64) -> Result<(), ErrorCode> {
        let mut parents = self
            .directory_parents
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;
        let metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        assert_eq!(metadata.get(&inode).unwrap().kind, FileKind::Directory);
        parents.insert(inode, new_parent);

        Ok(())
    }

    pub fn update_metadata_changed_time(&self, inode: u64) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        inode_attrs.last_metadata_changed = now();

        Ok(())
    }

    pub fn truncate(
        &self,
        inode: Inode,
        new_length: u64,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        if new_length > MAX_FILE_SIZE {
            return Err(ErrorCode::FileTooLarge);
        }

        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        if !check_access(
            inode_attrs.uid,
            inode_attrs.gid,
            inode_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        inode_attrs.size = new_length;
        inode_attrs.last_metadata_changed = now();
        inode_attrs.last_modified = now();

        Ok(())
    }

    // Remove a directory entry.
    // Returns the link inode, and a boolean indicating if processing is complete. If it is false
    // then it could not be determined if the link can be removed due to "stick bit".
    // In this case the inode's uid should be retrieved and this method retried, passing the
    // link_inode_and_uid argument.
    pub fn remove_link(
        &self,
        parent: u64,
        name: &str,
        // If provided, will preform "sticky bit" checks for the inode.
        link_inode_and_uid: Option<(u64, u32)>,
        context: UserContext,
    ) -> Result<(Inode, bool), ErrorCode> {
        let mut directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let parent_directory = directories
            .get_mut(&parent)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        let (inode, _) = parent_directory.get(name).ok_or(ErrorCode::DoesNotExist)?;

        if let Some((retrieved_inode, _)) = link_inode_and_uid {
            if retrieved_inode != *inode {
                // The inode that the client looked up is out of date (i.e. this link has been
                // deleted and recreated since then). Tell the client to look it up again.
                return Ok((*inode, false));
            }
        }

        let parent_attrs = metadata.get(&parent).ok_or(ErrorCode::InodeDoesNotExist)?;
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        let uid = context.uid();
        // "Sticky bit" handling
        if parent_attrs.mode & libc::S_ISVTX as u16 != 0 && uid != parent_attrs.uid && uid != 0 {
            if let Some((_, inode_uid)) = link_inode_and_uid {
                if uid != inode_uid {
                    return Err(ErrorCode::AccessDenied);
                }
            } else {
                // Sticky bit is on, and we need to check the inode's uid. Tell the client to lock
                // it and look up the uid.
                return Ok((*inode, false));
            }
        }

        let parent_attrs = metadata
            .get_mut(&parent)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        parent_attrs.last_metadata_changed = now();
        parent_attrs.last_modified = now();
        let (inode, _) = parent_directory
            .remove(name)
            .ok_or(ErrorCode::DoesNotExist)?;

        Ok((inode, true))
    }

    pub fn write(&self, inode: Inode, offset: u64, length: u32) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let inode_metadata = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;

        let current_length = inode_metadata.size;
        inode_metadata.size = max(current_length, u64::from(length) + offset);
        inode_metadata.last_metadata_changed = now();
        inode_metadata.last_modified = now();

        Ok(())
    }

    pub fn create_inode(
        &self,
        parent: Inode,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    ) -> Result<(Inode, InodeAttributes), ErrorCode> {
        let mut directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut parents = self
            .directory_parents
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;

        let inode = self.allocate_inode();
        let size = if kind == FileKind::Directory {
            BLOCK_SIZE
        } else {
            0
        };
        // Directories start with a link count of 2, since they have a self link
        let hardlinks = if kind == FileKind::Directory { 2 } else { 1 };
        let inode_metadata = InodeAttributes {
            inode,
            size,
            last_accessed: now(),
            last_modified: now(),
            last_metadata_changed: now(),
            kind,
            // TODO: suid/sgid not supported
            mode: mode & !(libc::S_ISUID | libc::S_ISGID) as u16,
            hardlinks,
            uid,
            gid,
            xattrs: Default::default(),
        };
        metadata.insert(inode, inode_metadata.clone());

        if kind == FileKind::Directory {
            directories.insert(inode, HashMap::new());
            parents.insert(inode, parent);
        }
        Ok((inode, inode_metadata))
    }

    // Returns an inode, if that inode's data should be deleted
    pub fn decrement_inode_link_count(
        &self,
        inode: Inode,
        count: u32,
    ) -> Result<Option<Inode>, ErrorCode> {
        let mut directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut parents = self
            .directory_parents
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;
        let mut metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;

        let inode_attrs = metadata
            .get_mut(&inode)
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        inode_attrs.hardlinks -= count;
        inode_attrs.last_metadata_changed = now();
        if inode_attrs.hardlinks == 0 {
            let is_directory = inode_attrs.kind == FileKind::Directory;
            metadata.remove(&inode);
            if is_directory {
                parents.remove(&inode);
                if let Some(entries) = directories.remove(&inode) {
                    assert!(entries.is_empty(), "Deleted a non-empty directory inode");
                }
            } else {
                // Only delete file contents if this is not a directory (directories don't have data contents)
                return Ok(Some(inode));
            }
        }

        Ok(None)
    }

    pub fn get_attributes(&self, inode: Inode) -> Result<(InodeAttributes, u32), ErrorCode> {
        // TODO: find a way to avoid this clone()
        let directories = self.directories.lock().map_err(|_| ErrorCode::Corrupted)?;
        let metadata = self.metadata.lock().map_err(|_| ErrorCode::Corrupted)?;
        let attributes = metadata
            .get(&inode)
            .cloned()
            .ok_or(ErrorCode::DoesNotExist)?;
        let directory_size = directories
            .get(&inode)
            .map(|entries| entries.len() as u32)
            .unwrap_or(0);

        Ok((attributes, directory_size))
    }

    fn allocate_inode(&self) -> u64 {
        self.next_inode
            .fetch_add(self.num_raft_groups, Ordering::SeqCst)
    }
}

fn now() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time before unix epoch");
    Timestamp::new(now.as_secs() as i64, now.subsec_nanos() as i32)
}
