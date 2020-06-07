use futures::Future;
use futures::FutureExt;

use crate::generated::ErrorCode;
use crate::peer_client::PeerClient;
use crate::storage::ROOT_INODE;
use crate::utils::{into_error_code, node_id_from_address, LengthPrefixedVec};
use futures::future::{err, join_all, Either};
use log::info;
use sha2::{Digest, Sha256};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::os::unix::io::IntoRawFd;
use std::path::{Path, PathBuf};
use std::{fs, io};
use walkdir::WalkDir;

pub const BLOCK_SIZE: u64 = 512;

pub struct DataStorage {
    node_ids: Vec<u64>,
    local_rank: u64,
    local_node_id: u64,
    local_data_dir: String,
    peers: HashMap<u64, PeerClient>,
}

// Convert to local index, or the nearest greater index on this (local_rank) node, if this index lives on another node
// If global_index is on the local_rank node, returns the local index of that byte
// Otherwise, selects the nearest global index greater than global_index, that is stored on local_rank node, and returns that local index
#[allow(clippy::comparison_chain)]
fn to_local_index_ceiling(global_index: u64, local_rank: u64, total_nodes: u64) -> u64 {
    let global_block = global_index / BLOCK_SIZE;
    let remainder_bytes = global_index % BLOCK_SIZE;
    let remainder_blocks = global_block % total_nodes;
    let blocks = global_block / total_nodes * BLOCK_SIZE;

    if local_rank < remainder_blocks {
        blocks + BLOCK_SIZE
    } else if local_rank == remainder_blocks {
        blocks + remainder_bytes
    } else {
        blocks
    }
}

// Return trues iff local_rank node stores the global index global_index
fn stores_index(global_index: u64, local_rank: u64, total_nodes: u64) -> bool {
    let global_block = global_index / BLOCK_SIZE;
    let remainder_blocks = global_block % total_nodes;

    local_rank == remainder_blocks
}

fn to_global_index(local_index: u64, local_rank: u64, total_nodes: u64) -> u64 {
    let stripes = local_index / BLOCK_SIZE;
    let remainder = local_index % BLOCK_SIZE;

    stripes * BLOCK_SIZE * total_nodes + local_rank * BLOCK_SIZE + remainder
}

// Abstraction of file storage. Files are split into blocks of BLOCK_SIZE, and stored in RAID0 across
// multiple nodes
impl DataStorage {
    pub fn new(
        local_node_id: u64,
        node_ids: &[u64],
        data_dir: &str,
        peers: &[SocketAddr],
    ) -> DataStorage {
        let mut sorted = node_ids.to_vec();
        sorted.sort();
        let local_rank = sorted.iter().position(|x| *x == local_node_id).unwrap() as u64;
        DataStorage {
            node_ids: sorted,
            local_node_id,
            local_rank,
            local_data_dir: data_dir.to_string(),
            peers: peers
                .iter()
                .map(|peer| (node_id_from_address(peer), PeerClient::new(*peer)))
                .collect(),
        }
    }

    pub fn local_data_checksum(&self) -> io::Result<Vec<u8>> {
        let mut hasher = Sha256::new();
        for entry in
            WalkDir::new(&self.local_data_dir).sort_by(|a, b| a.file_name().cmp(b.file_name()))
        {
            let entry = entry?;
            if entry.file_type().is_file() {
                // TODO hash the data and file attributes too
                let path_bytes = entry
                    .path()
                    .to_str()
                    .unwrap()
                    .trim_start_matches(&self.local_data_dir)
                    .as_bytes();
                hasher.write_all(path_bytes).unwrap();
            }
            // TODO handle other file types
        }
        return Ok(hasher.result().to_vec());
    }

    fn to_local_path(&self, path: &str) -> PathBuf {
        Path::new(&self.local_data_dir).join(path.trim_start_matches('/'))
    }

    // Writes the portions of data that should be stored locally to local storage
    pub fn write_local_blocks(
        &self,
        inode: u64,
        global_offset: u64,
        global_data: &[u8],
    ) -> io::Result<u32> {
        let local_index =
            to_local_index_ceiling(global_offset, self.local_rank, self.node_ids.len() as u64);
        let mut local_data = vec![];
        let mut start = if stores_index(global_offset, self.local_rank, self.node_ids.len() as u64)
        {
            let partial_first_block = BLOCK_SIZE - global_offset % BLOCK_SIZE;
            local_data.extend_from_slice(
                &global_data[0..min(partial_first_block as usize, global_data.len())],
            );
            (partial_first_block + (self.node_ids.len() - 1) as u64 * BLOCK_SIZE) as usize
        } else {
            (to_global_index(local_index, self.local_rank, self.node_ids.len() as u64)
                - global_offset) as usize
        };
        while start < global_data.len() {
            let end = min(start + BLOCK_SIZE as usize, global_data.len());
            local_data.extend_from_slice(&global_data[start..end]);
            start += self.node_ids.len() * BLOCK_SIZE as usize;
        }

        // TODO: hack
        let path = inode.to_string();
        let local_path = self.to_local_path(&path);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&local_path)?;

        file.seek(SeekFrom::Start(local_index))?;

        file.write_all(&local_data)?;
        return Ok(local_data.len() as u32);
    }

    pub(super) fn file_inode_exists(&self, inode: u64) -> bool {
        self.to_local_path(&inode.to_string()).exists()
    }

    pub fn read_raw(
        &self,
        inode: u64,
        global_offset: u64,
        global_size: u32,
    ) -> io::Result<LengthPrefixedVec> {
        assert_ne!(inode, ROOT_INODE);

        let local_start =
            to_local_index_ceiling(global_offset, self.local_rank, self.node_ids.len() as u64);
        let local_end = to_local_index_ceiling(
            global_offset + u64::from(global_size),
            self.local_rank,
            self.node_ids.len() as u64,
        );
        assert!(local_end >= local_start);

        let file = File::open(self.to_local_path(&inode.to_string()))?;
        // Requested read is from the client, so it could be past the end of the file
        // TODO: it seems like this could cause a bug, if reads and writes are interleaved, and one
        // replica whose bytes are in the middle of the read has already been truncated and therefore
        // returns an incomplete read
        let local_size = file.metadata()?.len();
        // Could underflow if file length is less than local_start
        let size = min(local_end, local_size).saturating_sub(local_start);

        let mut contents = LengthPrefixedVec::zeros(size as usize);
        file.read_exact_at(contents.bytes_mut(), local_start)?;

        Ok(contents)
    }

    pub fn read(
        &self,
        inode: u64,
        global_offset: u64,
        global_size: u32,
    ) -> impl Future<Output = Result<LengthPrefixedVec, ErrorCode>> {
        let local_data = match self.read_raw(inode, global_offset, global_size) {
            Ok(value) => value,
            Err(error) => {
                return Either::Left(err(into_error_code(error)));
            }
        };

        let mut remote_data_blocks = vec![];
        for node_id in self.node_ids.iter() {
            if *node_id == self.local_node_id {
                continue;
            }
            remote_data_blocks.push(
                self.peers[node_id]
                    .read_raw(inode, global_offset, global_size)
                    .map(|x| x.map_err(into_error_code)),
            );
        }

        let local_rank = self.local_rank;
        let result = join_all(remote_data_blocks).map(move |fetched_data_blocks| {
            let mut data_blocks: Vec<&[u8]> = vec![];
            let mut tmp_blocks = vec![];
            for x in fetched_data_blocks {
                tmp_blocks.push(x?);
            }
            for x in tmp_blocks.iter() {
                data_blocks.push(x);
            }
            data_blocks.insert(local_rank as usize, local_data.bytes());

            let mut result = LengthPrefixedVec::with_capacity(global_size as usize);
            let partial_first_block = BLOCK_SIZE - global_offset % BLOCK_SIZE;
            let first_block_size = data_blocks[0].len();
            let mut indices = vec![0; data_blocks.len()];
            let first_block_read = min(partial_first_block as usize, first_block_size);
            result.extend(&data_blocks[0][0..first_block_read]);
            indices[0] = first_block_read;

            let mut next_block = min(1, data_blocks.len() - 1);
            while indices[next_block] < data_blocks[next_block].len() {
                let index = indices[next_block];
                let remaining = data_blocks[next_block].len() - index;
                let block_read = min(BLOCK_SIZE as usize, remaining);
                result.extend(&data_blocks[next_block][index..(index + block_read)]);
                indices[next_block] += block_read;
                next_block += 1;
                next_block %= data_blocks.len();
            }

            Ok(result)
        });

        Either::Right(result)
    }

    pub fn truncate(&self, inode: u64, global_length: u64) -> io::Result<()> {
        let local_bytes =
            to_local_index_ceiling(global_length, self.local_rank, self.node_ids.len() as u64);
        let local_path = self.to_local_path(&inode.to_string());
        let file = File::create(&local_path).expect("Couldn't create file");
        file.set_len(local_bytes)?;

        Ok(())
    }

    pub fn fsync(&self, inode: u64) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        info!("Fsync'ing {}", inode);
        let local_path = self.to_local_path(&inode.to_string());
        let file = File::open(local_path).map_err(into_error_code)?;
        unsafe {
            libc::fsync(file.into_raw_fd());
        }
        Ok(())
    }

    pub fn delete(&self, inode: u64) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        let local_path = self.to_local_path(&inode.to_string());
        fs::remove_file(local_path).map_err(into_error_code)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::local::data_storage::{
        stores_index, to_global_index, to_local_index_ceiling, BLOCK_SIZE,
    };

    #[test]
    fn local_index_ceiling() {
        assert_eq!(to_local_index_ceiling(0, 0, 2), 0);
        assert_eq!(to_local_index_ceiling(0, 1, 2), 0);
        assert_eq!(to_local_index_ceiling(BLOCK_SIZE - 1, 0, 2), BLOCK_SIZE - 1);
        assert_eq!(to_local_index_ceiling(BLOCK_SIZE, 0, 2), BLOCK_SIZE);
        assert_eq!(to_local_index_ceiling(BLOCK_SIZE, 1, 2), 0);
        assert_eq!(
            to_local_index_ceiling(BLOCK_SIZE * 2 - 1, 1, 2),
            BLOCK_SIZE - 1
        );
        assert_eq!(to_local_index_ceiling(BLOCK_SIZE * 2, 0, 2), BLOCK_SIZE);

        assert_eq!(
            to_local_index_ceiling(BLOCK_SIZE * 2 + 1, 0, 2),
            BLOCK_SIZE + 1
        );
        assert_eq!(to_local_index_ceiling(BLOCK_SIZE * 2 + 1, 1, 2), BLOCK_SIZE);
        assert_eq!(
            to_local_index_ceiling(BLOCK_SIZE * 3 + 1, 1, 2),
            BLOCK_SIZE + 1
        );
    }

    #[test]
    fn round_trip() {
        for index in 0..(BLOCK_SIZE * 3) {
            println!("trying {}", index);
            assert_ne!(stores_index(index, 0, 2), stores_index(index, 1, 2));
            for rank in 0..=1 {
                if stores_index(index, rank, 2) {
                    let local_index = to_local_index_ceiling(index, rank, 2);
                    assert_eq!(index, to_global_index(local_index, rank, 2));
                }
            }
        }
    }
}