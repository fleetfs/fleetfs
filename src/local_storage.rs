use walkdir::WalkDir;

use sha2::{Digest, Sha256};
use std::{fs, io};

use crate::storage_node::LocalContext;

pub struct LocalStorage {
    context: LocalContext,
}

impl LocalStorage {
    pub fn new(context: LocalContext) -> LocalStorage {
        LocalStorage { context }
    }

    pub fn checksum(&self) -> io::Result<Vec<u8>> {
        let mut hasher = Sha256::new();
        for entry in
            WalkDir::new(&self.context.data_dir).sort_by(|a, b| a.file_name().cmp(b.file_name()))
        {
            let entry = entry?;
            if entry.file_type().is_file() {
                let mut file = fs::File::open(entry.path())?;

                // TODO hash the path and file attributes too
                io::copy(&mut file, &mut hasher)?;
            }
            // TODO handle other file types
        }
        return Ok(hasher.result().to_vec());
    }
}
