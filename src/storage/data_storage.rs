use futures::Future;

use crate::generated::ErrorCode;
use crate::peer_client::PeerClient;
use crate::storage::ROOT_INODE;
use crate::storage_node::LocalContext;
use crate::utils::into_error_code;
use futures::future::join_all;
use log::info;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::io::IntoRawFd;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub const BLOCK_SIZE: u64 = 512;

pub struct DataStorage {
    node_ids: Vec<u64>,
    local_rank: u64,
    local_node_id: u64,
    local_data_dir: String,
    peers: HashMap<u64, PeerClient>,
}

// Convert to local index, or the nearest lesser index on this (local_rank) node, if this index lives on another node
// If global_index is on the local_rank node, returns the local index of that byte
// Otherwise, selects the nearest global index less than global_index, that is stored on local_rank node, and returns that local index
// Returns None if there is no global index less than global_index, that is stored on local_rank node
fn to_local_index_floor(global_index: u64, local_rank: u64, total_nodes: u64) -> Option<u64> {
    let global_block = global_index / BLOCK_SIZE;
    let remainder_bytes = global_index % BLOCK_SIZE;
    let remainder_blocks = global_block % total_nodes;
    let blocks = global_block / total_nodes * BLOCK_SIZE;

    if local_rank < remainder_blocks {
        Some(blocks + BLOCK_SIZE - 1)
    } else if local_rank == remainder_blocks {
        Some(blocks + remainder_bytes)
    } else if blocks == 0 {
        None
    } else {
        Some(blocks - 1)
    }
}

// Convert to local index, or the nearest greater index on this (local_rank) node, if this index lives on another node
// If global_index is on the local_rank node, returns the local index of that byte
// Otherwise, selects the nearest global index greather than global_index, that is stored on local_rank node, and returns that local index
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
    pub fn new(local_node_id: u64, node_ids: &[u64], context: &LocalContext) -> DataStorage {
        // TODO: support sharding strategies besides RAID0 with 2 nodes
        assert_eq!(node_ids.len(), 2);
        let mut sorted = node_ids.to_vec();
        sorted.sort();
        let local_rank = sorted.iter().position(|x| *x == local_node_id).unwrap() as u64;
        DataStorage {
            node_ids: sorted,
            local_node_id,
            local_rank,
            local_data_dir: context.data_dir.clone(),
            peers: context
                .peers
                .iter()
                .map(|peer| (u64::from(peer.port()), PeerClient::new(*peer)))
                .collect(),
        }
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

    pub fn read_raw(
        &self,
        inode: u64,
        global_offset: u64,
        global_size: u32,
    ) -> io::Result<Vec<u8>> {
        assert_ne!(inode, ROOT_INODE);

        let local_start =
            to_local_index_ceiling(global_offset, self.local_rank, self.node_ids.len() as u64);
        let local_end = to_local_index_floor(
            global_offset + u64::from(global_size),
            self.local_rank,
            self.node_ids.len() as u64,
        )
        .unwrap_or(0);
        assert!(local_end >= local_start);

        let size = local_end - local_start;
        let file = File::open(self.to_local_path(&inode.to_string()))?;

        let mut contents = vec![0; size as usize];
        let bytes_read = file.read_at(&mut contents, local_start)?;
        contents.truncate(bytes_read);

        Ok(contents)
    }

    pub fn read(
        &self,
        inode: u64,
        global_offset: u64,
        global_size: u32,
    ) -> impl Future<Item = Vec<u8>, Error = ErrorCode> {
        // TODO: error handling
        let local_data = self.read_raw(inode, global_offset, global_size).unwrap();

        let mut remote_data_blocks = vec![];
        for node_id in self.node_ids.iter() {
            if *node_id == self.local_node_id {
                continue;
            }
            remote_data_blocks.push(self.peers[node_id].read_raw(
                inode,
                global_offset,
                global_size,
            ));
        }

        let local_rank = self.local_rank;
        join_all(remote_data_blocks)
            .map(move |mut data_blocks| {
                data_blocks.insert(local_rank as usize, local_data);

                let mut result = vec![];
                let partial_first_block = BLOCK_SIZE - global_offset % BLOCK_SIZE;
                let first_block_size = data_blocks[0].len();
                result.extend(
                    data_blocks[0].drain(0..min(partial_first_block as usize, first_block_size)),
                );

                let mut next_block = min(1, data_blocks.len() - 1);
                while !data_blocks[next_block].is_empty() {
                    let remaining = data_blocks[next_block].len();
                    result.extend(
                        data_blocks[next_block].drain(0..min(BLOCK_SIZE as usize, remaining)),
                    );
                    next_block += 1;
                    next_block %= data_blocks.len();
                }

                result
            })
            .map_err(into_error_code)
    }

    pub fn truncate(&self, inode: u64, global_length: u64) -> io::Result<()> {
        let local_bytes =
            to_local_index_floor(global_length, self.local_rank, self.node_ids.len() as u64)
                .unwrap_or(0);
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
    use crate::storage::data_storage::{
        stores_index, to_global_index, to_local_index_ceiling, to_local_index_floor, BLOCK_SIZE,
    };

    #[test]
    fn local_index_floor() {
        assert_eq!(to_local_index_floor(0, 0, 2), Some(0));
        assert_eq!(to_local_index_floor(0, 1, 2), None);
        assert_eq!(
            to_local_index_floor(BLOCK_SIZE - 1, 0, 2),
            Some(BLOCK_SIZE - 1)
        );
        assert_eq!(to_local_index_floor(BLOCK_SIZE, 0, 2), Some(BLOCK_SIZE - 1));
        assert_eq!(to_local_index_floor(BLOCK_SIZE, 1, 2), Some(0));
        assert_eq!(
            to_local_index_floor(BLOCK_SIZE * 2 - 1, 1, 2),
            Some(BLOCK_SIZE - 1)
        );
        assert_eq!(to_local_index_floor(BLOCK_SIZE * 2, 0, 2), Some(BLOCK_SIZE));

        assert_eq!(
            to_local_index_floor(BLOCK_SIZE * 2 + 1, 0, 2),
            Some(BLOCK_SIZE + 1)
        );
        assert_eq!(
            to_local_index_floor(BLOCK_SIZE * 2 + 1, 1, 2),
            Some(BLOCK_SIZE - 1)
        );
        assert_eq!(
            to_local_index_floor(BLOCK_SIZE * 3 + 1, 1, 2),
            Some(BLOCK_SIZE + 1)
        );
    }

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
                    let local_index = to_local_index_floor(index, rank, 2).unwrap();
                    assert_eq!(local_index, to_local_index_ceiling(index, rank, 2));
                    assert_eq!(index, to_global_index(local_index, rank, 2));
                }
            }
        }
    }
}
