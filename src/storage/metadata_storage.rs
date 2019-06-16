use std::cmp::max;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::generated::ErrorCode;
use crate::storage::data_storage::BLOCK_SIZE;

// TODO: add persistence
pub struct MetadataStorage {
    file_lengths: Mutex<HashMap<String, u64>>,
    uids: Mutex<HashMap<String, u32>>,
    gids: Mutex<HashMap<String, u32>>,
    xattrs: Mutex<HashMap<String, HashMap<String, Vec<u8>>>>,
}

impl MetadataStorage {
    pub fn new() -> MetadataStorage {
        MetadataStorage {
            file_lengths: Mutex::new(HashMap::new()),
            uids: Mutex::new(HashMap::new()),
            gids: Mutex::new(HashMap::new()),
            xattrs: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_xattr(&self, path: &str, key: &str) -> Option<Vec<u8>> {
        let xattrs = self.xattrs.lock().unwrap();
        xattrs.get(path)?.get(key).cloned()
    }

    pub fn list_xattrs(&self, path: &str) -> Vec<String> {
        let xattrs = self.xattrs.lock().unwrap();
        xattrs
            .get(path)
            .map(|attrs| attrs.keys().cloned().collect())
            .unwrap_or_else(|| vec![])
    }

    pub fn set_xattr(&self, path: &str, key: &str, value: &[u8]) {
        let mut xattrs = self.xattrs.lock().unwrap();
        if xattrs.contains_key(path) {
            xattrs
                .get_mut(path)
                .unwrap()
                .insert(key.to_string(), value.to_vec());
        } else {
            let mut attrs = HashMap::new();
            attrs.insert(key.to_string(), value.to_vec());
            xattrs.insert(path.to_string(), attrs);
        }
    }

    pub fn remove_xattr(&self, path: &str, key: &str) {
        let mut xattrs = self.xattrs.lock().unwrap();
        xattrs.get_mut(path).map(|attrs| attrs.remove(key));
    }

    // TODO: should have some error handling
    pub fn get_length(&self, path: &str) -> Option<u64> {
        let file_lengths = self.file_lengths.lock().unwrap();

        file_lengths.get(path).cloned()
    }

    pub fn get_uid(&self, path: &str) -> Option<u32> {
        let uids = self.uids.lock().unwrap();

        uids.get(path).cloned()
    }

    pub fn get_gid(&self, path: &str) -> Option<u32> {
        let gids = self.gids.lock().unwrap();

        gids.get(path).cloned()
    }

    // TODO: should have some error handling
    pub fn chown(&self, path: &str, uid: Option<u32>, gid: Option<u32>) -> Result<(), ErrorCode> {
        if let Some(uid) = uid {
            let mut uids = self.uids.lock().unwrap();
            uids.insert(path.to_string(), uid);
        }
        if let Some(gid) = gid {
            let mut gids = self.gids.lock().unwrap();
            gids.insert(path.to_string(), gid);
        }

        Ok(())
    }

    pub fn hardlink(&self, path: &str, new_path: &str) {
        // TODO: need to switch this to use inodes. This doesn't have the right semantics, since
        // it only copies the size on creation
        let mut file_lengths = self.file_lengths.lock().unwrap();

        if let Some(&current_length) = file_lengths.get(path) {
            file_lengths.insert(new_path.to_string(), current_length);
        }
    }

    pub fn mkdir(&self, path: &str) {
        let mut file_lengths = self.file_lengths.lock().unwrap();
        file_lengths.insert(path.to_string(), BLOCK_SIZE);
    }

    pub fn rename(&self, path: &str, new_path: &str) {
        let mut file_lengths = self.file_lengths.lock().unwrap();

        if let Some(current_length) = file_lengths.remove(path) {
            file_lengths.insert(new_path.to_string(), current_length);
        }
    }

    // TODO: should have some error handling
    pub fn truncate(&self, path: &str, new_length: u64) {
        let mut file_lengths = self.file_lengths.lock().unwrap();
        file_lengths.insert(path.to_string(), new_length);
    }

    // TODO: should have some error handling
    pub fn unlink(&self, path: &str) {
        let mut file_lengths = self.file_lengths.lock().unwrap();

        file_lengths.remove(path);
    }

    // TODO: should have some error handling
    pub fn rmdir(&self, path: &str) {
        let mut file_lengths = self.file_lengths.lock().unwrap();

        file_lengths.remove(path);
    }

    // TODO: should have some error handling
    pub fn write(&self, path: &str, offset: u64, length: u32) {
        let mut file_lengths = self.file_lengths.lock().unwrap();

        let current_length = *file_lengths.get(path).unwrap_or(&0);
        file_lengths.insert(
            path.to_string(),
            max(current_length, u64::from(length) + offset),
        );
    }
}
