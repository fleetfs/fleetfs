use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::generated::ErrorCode;
use crate::storage::data_storage::BLOCK_SIZE;
use std::path::Path;

pub const ROOT_INODE: u64 = 0;

type Inode = u64;

// TODO: add persistence
pub struct MetadataStorage {
    file_lengths: Mutex<HashMap<Inode, u64>>,
    uids: Mutex<HashMap<Inode, u32>>,
    gids: Mutex<HashMap<Inode, u32>>,
    xattrs: Mutex<HashMap<Inode, HashMap<String, Vec<u8>>>>,
    // Directory contents. This is considered metadata rather than data,
    // because it's replicated to all nodes to allow better searching.
    // Maybe we should revisit that design?
    // Maps directory inodes to maps of name -> inode
    directories: Mutex<HashMap<Inode, HashMap<String, Inode>>>,
    // Raft guarantees that operations are performed in the same order across all nodes
    // which means that all nodes have the same value for this counter
    next_inode: AtomicU64,
}

impl MetadataStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> MetadataStorage {
        let mut directories = HashMap::new();
        directories.insert(ROOT_INODE, HashMap::new());

        MetadataStorage {
            file_lengths: Mutex::new(HashMap::new()),
            uids: Mutex::new(HashMap::new()),
            gids: Mutex::new(HashMap::new()),
            xattrs: Mutex::new(HashMap::new()),
            directories: Mutex::new(directories),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        }
    }

    pub fn lookup(&self, parent: Inode, name: &str) -> Option<Inode> {
        let directories = self.directories.lock().unwrap();
        directories.get(&parent).unwrap().get(name).cloned()
    }

    fn lookup_path(&self, path: &str) -> Option<Inode> {
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
        let xattrs = self.xattrs.lock().unwrap();
        xattrs.get(&inode)?.get(key).cloned()
    }

    pub fn list_xattrs(&self, inode: Inode) -> Vec<String> {
        let xattrs = self.xattrs.lock().unwrap();
        xattrs
            .get(&inode)
            .map(|attrs| attrs.keys().cloned().collect())
            .unwrap_or_else(|| vec![])
    }

    pub fn set_xattr(&self, inode: Inode, key: &str, value: &[u8]) {
        let mut xattrs = self.xattrs.lock().unwrap();
        if xattrs.contains_key(&inode) {
            xattrs
                .get_mut(&inode)
                .unwrap()
                .insert(key.to_string(), value.to_vec());
        } else {
            let mut attrs = HashMap::new();
            attrs.insert(key.to_string(), value.to_vec());
            xattrs.insert(inode, attrs);
        }
    }

    pub fn remove_xattr(&self, inode: Inode, key: &str) {
        let mut xattrs = self.xattrs.lock().unwrap();
        xattrs.get_mut(&inode).map(|attrs| attrs.remove(key));
    }

    // TODO: should have some error handling
    pub fn get_length(&self, path: &str) -> Option<u64> {
        let file_lengths = self.file_lengths.lock().unwrap();
        let inode = self.lookup_path(path).unwrap();

        file_lengths.get(&inode).cloned()
    }

    pub fn get_uid(&self, path: &str) -> Option<u32> {
        let uids = self.uids.lock().unwrap();
        let inode = self.lookup_path(path).unwrap();

        uids.get(&inode).cloned()
    }

    pub fn get_gid(&self, path: &str) -> Option<u32> {
        let gids = self.gids.lock().unwrap();
        let inode = self.lookup_path(path).unwrap();

        gids.get(&inode).cloned()
    }

    // TODO: should have some error handling
    pub fn chown(&self, inode: Inode, uid: Option<u32>, gid: Option<u32>) -> Result<(), ErrorCode> {
        if let Some(uid) = uid {
            let mut uids = self.uids.lock().unwrap();
            uids.insert(inode, uid);
        }
        if let Some(gid) = gid {
            let mut gids = self.gids.lock().unwrap();
            gids.insert(inode, gid);
        }

        Ok(())
    }

    pub fn hardlink(&self, path: &str, new_path: &str) {
        // TODO: need to switch this to use inodes. This doesn't have the right semantics, since
        // it only copies the size on creation
        let inode = self.lookup_path(path).unwrap();

        let new_parent = self.lookup_parent(new_path).unwrap();
        let new_basename = basename(new_path);
        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_basename, inode);
    }

    pub fn mkdir(&self, path: &str) {
        let mut file_lengths = self.file_lengths.lock().unwrap();

        let inode = self.allocate_inode();
        let parent = self.lookup_parent(path).unwrap();
        let basename = basename(path);
        let mut directories = self.directories.lock().unwrap();
        directories
            .get_mut(&parent)
            .unwrap()
            .insert(basename, inode);
        directories.insert(inode, HashMap::new());

        file_lengths.insert(inode, BLOCK_SIZE);
    }

    pub fn rename(&self, path: &str, new_path: &str) {
        let old_basename = basename(path);
        let new_basename = basename(new_path);
        let old_parent = self.lookup_parent(path).unwrap();
        let new_parent = self.lookup_parent(new_path).unwrap();
        let mut directories = self.directories.lock().unwrap();
        let inode = directories
            .get_mut(&old_parent)
            .unwrap()
            .remove(&old_basename)
            .unwrap();
        directories
            .get_mut(&new_parent)
            .unwrap()
            .insert(new_basename, inode);
    }

    // TODO: should have some error handling
    pub fn truncate(&self, path: &str, new_length: u64) {
        let inode = self.lookup_path(path).unwrap();
        let mut file_lengths = self.file_lengths.lock().unwrap();
        file_lengths.insert(inode, new_length);
    }

    // TODO: should have some error handling
    pub fn unlink(&self, path: &str) {
        let inode = self.lookup_path(path).unwrap();

        let mut file_lengths = self.file_lengths.lock().unwrap();
        file_lengths.remove(&inode);

        let parent = self.lookup_parent(path).unwrap();
        let basename = basename(path);
        let mut directories = self.directories.lock().unwrap();
        directories.get_mut(&parent).unwrap().remove(&basename);
    }

    // TODO: should have some error handling
    pub fn rmdir(&self, path: &str) {
        let inode = self.lookup_path(path).unwrap();

        let mut file_lengths = self.file_lengths.lock().unwrap();
        file_lengths.remove(&inode);

        let mut directories = self.directories.lock().unwrap();
        directories.remove(&inode);
    }

    // TODO: should have some error handling
    pub fn write(&self, path: &str, offset: u64, length: u32) {
        // TODO: awful hack, because client doesn't create files properly
        self.create(path);

        let inode = self.lookup_path(path).unwrap();

        let mut file_lengths = self.file_lengths.lock().unwrap();

        let current_length = *file_lengths.get(&inode).unwrap_or(&0);
        file_lengths.insert(inode, max(current_length, u64::from(length) + offset));
    }

    pub fn create(&self, path: &str) {
        if self.lookup_path(path).is_none() {
            let parent = self.lookup_parent(path).unwrap();
            let basename = basename(path);
            let mut directories = self.directories.lock().unwrap();
            directories
                .get_mut(&parent)
                .unwrap()
                .insert(basename, self.allocate_inode());
        }
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
