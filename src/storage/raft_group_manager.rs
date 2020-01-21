use crate::storage::raft_node::RaftNode;
use crate::storage_node::LocalContext;
use rand::seq::IteratorRandom;
use std::collections::HashMap;

// Manages all the local node's raft groups
pub struct LocalRaftGroupManager {
    // Mapping of rgroup ids to instances
    groups: HashMap<u16, RaftNode>,
}

impl LocalRaftGroupManager {
    pub fn new(rgroups: u16, context: LocalContext) -> LocalRaftGroupManager {
        let mut groups = HashMap::new();
        // TODO: groups should be divided among nodes. Not every group on every node.
        for i in 0..rgroups {
            groups.insert(i, RaftNode::new(context.clone(), i, rgroups));
        }

        LocalRaftGroupManager { groups }
    }

    pub fn all_groups(&self) -> impl Iterator<Item = &RaftNode> {
        self.groups.values()
    }

    pub fn lookup_by_raft_group(&self, raft_group: u16) -> &RaftNode {
        &self.groups[&raft_group]
    }

    pub fn least_loaded_group(&self) -> &RaftNode {
        // TODO: actually load balance
        &self
            .groups
            .values()
            .choose(&mut rand::thread_rng())
            .unwrap()
    }

    pub fn lookup_by_inode(&self, inode: u64) -> &RaftNode {
        // TODO: assumes every storage node has every rgroup
        &self.groups[&((inode % self.groups.len() as u64) as u16)]
    }

    // TODO: sharding of data across rgroups is not implemented yet
    pub fn lookup_by_block(&self, inode: u64, _block: u64) -> &RaftNode {
        self.lookup_by_inode(inode)
    }

    pub fn background_tick(&self) {
        for node in self.groups.values() {
            node.background_tick();
        }
    }
}
