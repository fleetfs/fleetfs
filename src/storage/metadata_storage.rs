use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::generated::{ErrorCode, FileKind, Timestamp};
use crate::storage::data_storage::BLOCK_SIZE;
use fuse::FUSE_ROOT_ID;

pub const ROOT_INODE: u64 = FUSE_ROOT_ID;

type Inode = u64;
type DirectoryDescriptor = HashMap<String, (Inode, FileKind)>;

#[derive(Clone)]
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
            directory_parents: Mutex::new(parents),
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
    pub fn chmod(&self, inode: Inode, mode: u32) {
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

    pub fn hardlink(&self, inode: Inode, new_parent: u64, new_name: &str) {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_attrs = metadata.get_mut(&inode).unwrap();
        inode_attrs.hardlinks += 1;

        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_name.to_string(), (inode, inode_attrs.kind));
    }

    pub fn mkdir(&self, parent: u64, name: &str, uid: u32, gid: u32, mode: u16) {
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

    pub fn rename(&self, parent: u64, name: &str, new_parent: u64, new_name: &str) {
        let mut directories = self.directories.lock().unwrap();
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
    }

    // TODO: should have some error handling
    pub fn truncate(&self, inode: Inode, new_length: u64) {
        let mut metadata = self.metadata.lock().unwrap();
        metadata.get_mut(&inode).unwrap().size = new_length;
    }

    // TODO: should have some error handling
    // Returns an inode, if that inode's data should be deleted
    pub fn unlink(&self, parent: u64, name: &str) -> Option<Inode> {
        let mut directories = self.directories.lock().unwrap();
        let (inode, _) = directories.get_mut(&parent).unwrap().remove(name).unwrap();

        let mut metadata = self.metadata.lock().unwrap();
        metadata.get_mut(&inode).unwrap().hardlinks -= 1;
        if metadata.get(&inode).unwrap().hardlinks == 0 {
            metadata.remove(&inode);
            return Some(inode);
        }

        None
    }

    // TODO: should have some error handling
    pub fn rmdir(&self, parent: u64, name: &str) {
        let mut directories = self.directories.lock().unwrap();
        if let Some((inode, _)) = directories.get_mut(&parent).unwrap().remove(name) {
            directories.remove(&inode);
            let mut metadata = self.metadata.lock().unwrap();
            metadata.remove(&inode);
            let mut parents = self.directory_parents.lock().unwrap();
            parents.remove(&inode);
        }
    }

    // TODO: should have some error handling
    pub fn write(&self, inode: Inode, offset: u64, length: u32) {
        let mut metadata = self.metadata.lock().unwrap();
        let inode_metadata = metadata.get_mut(&inode).unwrap();
        let current_length = inode_metadata.size;
        inode_metadata.size = max(current_length, u64::from(length) + offset)
    }

    pub fn create(
        &self,
        parent: Inode,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<(Inode, InodeAttributes), ErrorCode> {
        if self.lookup(parent, name).is_none() {
            let inode = self.allocate_inode();
            let mut directories = self.directories.lock().unwrap();
            directories
                .get_mut(&parent)
                .unwrap()
                .insert(name.to_string(), (inode, FileKind::File));

            let inode_metadata = InodeAttributes {
                inode,
                size: 0,
                last_accessed: Timestamp::new(0, 0),
                last_modified: Timestamp::new(0, 0),
                last_metadata_changed: Timestamp::new(0, 0),
                kind: FileKind::File,
                mode,
                hardlinks: 1,
                uid,
                gid,
                xattrs: Default::default(),
            };
            let mut metadata = self.metadata.lock().unwrap();
            metadata.insert(inode, inode_metadata.clone());
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
