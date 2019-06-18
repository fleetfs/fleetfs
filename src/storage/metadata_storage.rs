use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::generated::{ErrorCode, FileKind, Timestamp};
use crate::storage::data_storage::BLOCK_SIZE;
use std::path::Path;

pub const ROOT_INODE: u64 = 0;

type Inode = u64;
type DirectoryDescriptor = HashMap<String, (Inode, FileKind)>;

#[derive(Clone)]
pub struct InodeAttributes {
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
    // Raft guarantees that operations are performed in the same order across all nodes
    // which means that all nodes have the same value for this counter
    next_inode: AtomicU64,
}

impl MetadataStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> MetadataStorage {
        let mut directories = HashMap::new();
        directories.insert(ROOT_INODE, HashMap::new());

        let mut metadata = HashMap::new();
        metadata.insert(
            ROOT_INODE,
            InodeAttributes {
                size: 0,
                last_accessed: Timestamp::new(0, 0),
                last_modified: Timestamp::new(0, 0),
                last_metadata_changed: Timestamp::new(0, 0),
                kind: FileKind::Directory,
                mode: 0o755,
                hardlinks: 2,
                uid: 0,
                gid: 0,
                xattrs: Default::default(),
            },
        );

        MetadataStorage {
            metadata: Mutex::new(metadata),
            directories: Mutex::new(directories),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        }
    }

    pub fn lookup(&self, parent: Inode, name: &str) -> Option<Inode> {
        let directories = self.directories.lock().unwrap();
        directories
            .get(&parent)
            .unwrap()
            .get(name)
            .map(|(inode, _)| *inode)
    }

    pub fn lookup_path(&self, path: &str) -> Option<Inode> {
        if path.is_empty() {
            return Some(ROOT_INODE);
        }

        let parent = self.lookup_parent(path)?;
        let basename = basename(path);
        self.lookup(parent, &basename)
    }

    fn lookup_parent(&self, path: &str) -> Option<Inode> {
        match Path::new(path)
            .parent()
            .map(|x| x.to_str().unwrap().to_string())
        {
            None => Some(ROOT_INODE),
            Some(parent) => self.lookup_path(&parent),
        }
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
        metadata
            .get_mut(&inode)
            .and_then(|x| x.xattrs.insert(key.to_string(), value.to_vec()));
    }

    pub fn remove_xattr(&self, inode: Inode, key: &str) {
        let mut metadata = self.metadata.lock().unwrap();
        metadata.get_mut(&inode).and_then(|x| x.xattrs.remove(key));
    }

    pub fn readdir(&self, inode: Inode) -> Result<Vec<(String, FileKind)>, ErrorCode> {
        let directories = self.directories.lock().unwrap();

        if let Some(entries) = directories.get(&inode) {
            let result = entries
                .iter()
                .map(|(name, (_, kind))| (name.clone(), *kind))
                .collect();
            Ok(result)
        } else {
            Err(ErrorCode::DoesNotExist)
        }
    }

    pub fn get_uid(&self, path: &str) -> Option<u32> {
        let inode = self.lookup_path(path).unwrap();
        let metadata = self.metadata.lock().unwrap();

        metadata.get(&inode).map(|x| x.uid)
    }

    pub fn get_gid(&self, path: &str) -> Option<u32> {
        let inode = self.lookup_path(path).unwrap();
        let metadata = self.metadata.lock().unwrap();

        metadata.get(&inode).map(|x| x.gid)
    }

    pub fn utimens(
        &self,
        inode: Inode,
        uid: u32,
        atime: Option<&Timestamp>,
        mtime: Option<&Timestamp>,
    ) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();

        if let Some(inode_metadata) = metadata.get_mut(&inode) {
            // Non-owners are only allowed to change atime & mtime to current:
            // http://man7.org/linux/man-pages/man2/utimensat.2.html
            if inode_metadata.uid != uid
                && uid != 0
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

    // TODO: should have some error handling
    pub fn chmod(&self, path: &str, mode: u32) {
        let inode = self.lookup_path(path).unwrap();
        let mut metadata = self.metadata.lock().unwrap();
        metadata.get_mut(&inode).unwrap().mode = mode as u16;
    }

    // TODO: should have some error handling
    pub fn chown(&self, inode: Inode, uid: Option<u32>, gid: Option<u32>) -> Result<(), ErrorCode> {
        let mut metadata = self.metadata.lock().unwrap();

        let inode_metadata = metadata.get_mut(&inode).unwrap();
        if let Some(uid) = uid {
            inode_metadata.uid = uid;
        }
        if let Some(gid) = gid {
            inode_metadata.gid = gid;
        }

        Ok(())
    }

    pub fn hardlink(&self, path: &str, new_path: &str) {
        // TODO: need to switch this to use inodes. This doesn't have the right semantics, since
        // it only copies the size on creation
        let inode = self.lookup_path(path).unwrap();

        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        inode_attrs.hardlinks += 1;

        let new_parent = self.lookup_parent(new_path).unwrap();
        let new_basename = basename(new_path);
        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_basename, (inode, inode_attrs.kind));
    }

    pub fn mkdir(&self, path: &str, uid: u32, gid: u32, mode: u16) {
        let inode = self.allocate_inode();
        let parent = self.lookup_parent(path).unwrap();
        let basename = basename(path);
        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&parent)
            .unwrap()
            .insert(basename, (inode, FileKind::Directory));
        directories.insert(inode, HashMap::new());

        let inode_metadata = InodeAttributes {
            size: BLOCK_SIZE,
            last_accessed: Timestamp::new(0, 0),
            last_modified: Timestamp::new(0, 0),
            last_metadata_changed: Timestamp::new(0, 0),
            kind: FileKind::Directory,
            mode,
            hardlinks: 2,
            uid,
            gid,
            xattrs: Default::default(),
        };
        let mut metadata = self.metadata.lock().unwrap();
        metadata.insert(inode, inode_metadata);
    }

    pub fn rename(&self, path: &str, new_path: &str) {
        let old_basename = basename(path);
        let new_basename = basename(new_path);
        let old_parent = self.lookup_parent(path).unwrap();
        let new_parent = self.lookup_parent(new_path).unwrap();
        let mut directories = self.directories.lock().unwrap();
        let entry = directories
            .get_mut(&old_parent)
            .unwrap()
            .remove(&old_basename)
            .unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_basename, entry);
    }

    // TODO: should have some error handling
    pub fn truncate(&self, path: &str, new_length: u64) {
        let inode = self.lookup_path(path).unwrap();
        let mut metadata = self.metadata.lock().unwrap();
        metadata.get_mut(&inode).unwrap().size = new_length;
    }

    // TODO: should have some error handling
    pub fn unlink(&self, path: &str) {
        let inode = self.lookup_path(path).unwrap();

        let parent = self.lookup_parent(path).unwrap();
        let basename = basename(path);
        let mut directories = self.directories.lock().unwrap();
        directories.get_mut(&parent).unwrap().remove(&basename);

        let mut metadata = self.metadata.lock().unwrap();
        metadata.get_mut(&inode).unwrap().hardlinks -= 1;
        if metadata.get(&inode).unwrap().hardlinks == 0 {
            metadata.remove(&inode);
        }
    }

    // TODO: should have some error handling
    pub fn rmdir(&self, path: &str) {
        let inode = self.lookup_path(path).unwrap();

        let mut metadata = self.metadata.lock().unwrap();
        metadata.remove(&inode);

        let mut directories = self.directories.lock().unwrap();
        directories.remove(&inode);
    }

    // TODO: should have some error handling
    pub fn write(&self, path: &str, offset: u64, length: u32) {
        // TODO: awful hack, because client doesn't create files properly
        self.create(path);

        let inode = self.lookup_path(path).unwrap();
        let mut metadata = self.metadata.lock().unwrap();
        let inode_metadata = metadata.get_mut(&inode).unwrap();
        let current_length = inode_metadata.size;
        inode_metadata.size = max(current_length, u64::from(length) + offset)
    }

    pub fn create(&self, path: &str) {
        if self.lookup_path(path).is_none() {
            let parent = self.lookup_parent(path).unwrap();
            let basename = basename(path);
            let inode = self.allocate_inode();
            let mut directories = self.directories.lock().unwrap();
            directories
                .get_mut(&parent)
                .unwrap()
                .insert(basename, (inode, FileKind::File));

            let inode_metadata = InodeAttributes {
                size: 0,
                last_accessed: Timestamp::new(0, 0),
                last_modified: Timestamp::new(0, 0),
                last_metadata_changed: Timestamp::new(0, 0),
                kind: FileKind::File,
                mode: 0o755,
                hardlinks: 1,
                uid: 0, // TODO
                gid: 0,
                xattrs: Default::default(),
            };
            let mut metadata = self.metadata.lock().unwrap();
            metadata.insert(inode, inode_metadata);
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

fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}
