use futures::Future;
use futures::FutureExt;

use crate::base::{CommitId, ErrorCode};
use crate::client::PeerClient;
use crate::storage::local::error_helper::into_error_code;
use crate::storage::ROOT_INODE;
use futures::future::{err, join_all, Either};
use log::info;
use sha2::{Digest, Sha256};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::{fs, io};
use walkdir::WalkDir;

pub const BLOCK_SIZE: u64 = 512;

pub struct DataStorage<T: PeerClient> {
    node_ids: Vec<u64>,
    local_rank: u64,
    local_node_id: u64,
    local_data_dir: String,
    peers: HashMap<u64, T>,
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
impl<T: PeerClient> DataStorage<T> {
    pub fn new(local_node_id: u64, data_dir: &str, peers: HashMap<u64, T>) -> DataStorage<T> {
        let mut sorted: Vec<u64> = peers.keys().cloned().collect();
        sorted.push(local_node_id);
        sorted.sort_unstable();
        sorted.dedup_by(|a, b| a == b);
        let local_rank = sorted.iter().position(|x| *x == local_node_id).unwrap() as u64;
        DataStorage {
            node_ids: sorted,
            local_node_id,
            local_rank,
            local_data_dir: data_dir.to_string(),
            peers,
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
        Ok(hasher.finalize().to_vec())
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
        if local_data.is_empty() {
            // Ensure that the local file has been zero-extended properly.
            // Otherwise a small write that leaves a hole in the file may not be
            // zero filled correctly
            // TODO: this should be optimized to store sparely written files more optimally
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&local_path)
                .expect("Couldn't create file");
            let local_size = file.metadata()?.len();
            if local_size < local_index {
                file.set_len(local_index)?;
            }
        } else {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&local_path)?;

            file.seek(SeekFrom::Start(local_index))?;

            file.write_all(&local_data)?;
        }
        Ok(local_data.len() as u32)
    }

    pub(super) fn file_inode_exists(&self, inode: u64) -> bool {
        self.to_local_path(&inode.to_string()).exists()
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

        let mut contents = vec![0u8; size as usize];
        file.read_exact_at(&mut contents, local_start)?;

        Ok(contents)
    }

    pub fn read(
        &self,
        inode: u64,
        global_offset: u64,
        global_size: u32,
        required_commit: CommitId,
    ) -> impl Future<Output = Result<Vec<u8>, ErrorCode>> + '_ {
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
                    .read_raw(inode, global_offset, global_size, required_commit)
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
            data_blocks.insert(local_rank as usize, &local_data);

            let mut result = Vec::with_capacity(global_size as usize);
            let partial_first_block = BLOCK_SIZE - global_offset % BLOCK_SIZE;
            let first_block_index = (global_offset / BLOCK_SIZE) as usize % data_blocks.len();
            let first_block_size = data_blocks[first_block_index].len();
            let mut indices = vec![0; data_blocks.len()];
            let first_block_read = min(partial_first_block as usize, first_block_size);
            result.extend(&data_blocks[first_block_index][0..first_block_read]);
            indices[first_block_index] = first_block_read;

            let mut next_block = (first_block_index + 1) % data_blocks.len();
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
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(local_path)
            .expect("Couldn't create file");
        file.set_len(local_bytes)?;

        Ok(())
    }

    pub fn fsync(&self, inode: u64) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        info!("Fsync'ing {}", inode);
        let local_path = self.to_local_path(&inode.to_string());
        let file = File::open(local_path).map_err(into_error_code)?;
        file.sync_all().map_err(into_error_code)?;
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
    use crate::base::CommitId;
    use crate::client::PeerClient;
    use crate::storage::local::data_storage::{
        stores_index, to_global_index, to_local_index_ceiling, DataStorage, BLOCK_SIZE,
    };
    use crate::ErrorCode;
    use futures::future::{ready, BoxFuture};
    use futures_util::future::FutureExt;
    use raft::eraftpb::Message;
    use rand::Rng;
    use rkyv::util::AlignedVec;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs;
    use std::io::Error;
    use tempfile::tempdir;

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

    #[test]
    fn sharding_integration() {
        let nodes = 6;
        let tmp_dir = tempdir().unwrap();

        let cluster = FakeCluster {
            data_stores: RefCell::new(HashMap::new()),
        };

        let mut clients = HashMap::new();
        for i in 0..nodes {
            clients.insert(
                i,
                FakePeerClient {
                    cluster: &cluster,
                    node_id: i,
                },
            );
        }

        for i in 0..nodes {
            let storage_path = tmp_dir.path().join(i.to_string());
            fs::create_dir(&storage_path).unwrap();
            cluster.data_stores.borrow_mut().insert(
                i,
                DataStorage::new(i, storage_path.to_str().unwrap(), clients.clone()),
            );
        }

        let mut data = vec![0u8; 20 * 1024];
        for element in &mut data {
            *element = rand::thread_rng().gen();
        }

        cluster.write(0, 0, &data);
        cluster.read_assert(0, 0, data.len() as u32, &data);

        // Do a bunch of random writes
        let num_writes = 1000;
        for _ in 0..num_writes {
            let size = rand::thread_rng().gen_range(0..data.len());
            let offset = rand::thread_rng().gen_range(0..(data.len() - size));
            for i in 0..size {
                data[offset + i] = data[offset + i].wrapping_add(1);
            }
            cluster.write(0, offset as u64, &data[offset..(offset + size)]);
            cluster.read_assert(
                0,
                offset as u64,
                size as u32,
                &data[offset..(offset + size)],
            );
            cluster.read_assert(0, 0, data.len() as u32, &data);
        }
    }

    struct FakeCluster<'a> {
        data_stores: RefCell<HashMap<u64, DataStorage<FakePeerClient<'a>>>>,
    }

    impl<'a> FakeCluster<'a> {
        fn write(&self, inode: u64, offset: u64, data: &[u8]) {
            for s in self.data_stores.borrow().values() {
                s.write_local_blocks(inode, offset, data).unwrap();
            }
        }

        fn read_assert(&self, inode: u64, offset: u64, size: u32, expected_data: &[u8]) {
            for s in self.data_stores.borrow().values() {
                let result = s
                    .read(inode, offset, size, CommitId::new(0, 0))
                    .now_or_never()
                    .unwrap();
                assert_eq!(result.unwrap(), expected_data);
            }
        }
    }

    #[derive(Clone)]
    struct FakePeerClient<'a> {
        cluster: &'a FakeCluster<'a>,
        node_id: u64,
    }

    impl<'a> PeerClient for FakePeerClient<'a> {
        fn send_raw<T: AsRef<[u8]> + Send + 'static>(
            &self,
            _data: T,
        ) -> BoxFuture<'static, Result<AlignedVec, Error>> {
            unimplemented!()
        }

        fn send_raft_message(&self, _raft_group: u16, _message: Message) -> BoxFuture<'static, ()> {
            unimplemented!()
        }

        fn get_latest_commit(&self, _raft_group: u16) -> BoxFuture<'static, Result<u64, Error>> {
            unimplemented!()
        }

        fn filesystem_checksum(
            &self,
        ) -> BoxFuture<'static, Result<HashMap<u16, Vec<u8>>, ErrorCode>> {
            unimplemented!()
        }

        fn read_raw(
            &self,
            inode: u64,
            offset: u64,
            size: u32,
            _required_commit: CommitId,
        ) -> BoxFuture<'static, Result<Vec<u8>, Error>> {
            let data = self
                .cluster
                .data_stores
                .borrow()
                .get(&self.node_id)
                .unwrap()
                .read_raw(inode, offset, size)
                .unwrap();
            ready(Ok(data)).boxed()
        }
    }
}
