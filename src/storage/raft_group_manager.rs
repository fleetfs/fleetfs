use crate::base::node_contains_raft_group;
use crate::base::LocalContext;
use crate::storage::raft_node::RaftNode;
use std::collections::HashMap;

// Manages all the local node's raft groups
pub struct LocalRaftGroupManager {
    context: LocalContext,
    total_raft_groups: u16,
    // Mapping of rgroup ids to instances
    groups: HashMap<u16, RaftNode>,
}

impl LocalRaftGroupManager {
    pub fn new(rgroups: u16, context: LocalContext) -> LocalRaftGroupManager {
        let mut groups = HashMap::new();
        // TODO: groups should be divided among nodes. Not every group on every node.
        for i in 0..rgroups {
            if node_contains_raft_group(
                context.node_index(),
                context.total_nodes(),
                i,
                context.replicas_per_raft_group,
            ) {
                groups.insert(i, RaftNode::new(context.clone(), i, rgroups));
            }
        }

        LocalRaftGroupManager {
            context,
            total_raft_groups: rgroups,
            groups,
        }
    }

    // Returns true if the raft group for the given inode is stored on this node
    pub fn inode_stored_locally(&self, inode: u64) -> bool {
        let raft_group_id = inode % self.total_raft_groups as u64;
        node_contains_raft_group(
            self.context.node_index(),
            self.context.total_nodes(),
            raft_group_id as u16,
            self.context.replicas_per_raft_group,
        )
    }

    pub fn all_groups(&self) -> impl Iterator<Item = &RaftNode> {
        self.groups.values()
    }

    pub fn has_raft_group(&self, raft_group: u16) -> bool {
        self.groups.contains_key(&raft_group)
    }

    pub fn lookup_by_raft_group(&self, raft_group: u16) -> &RaftNode {
        &self.groups[&raft_group]
    }

    pub fn lookup_by_inode(&self, inode: u64) -> &RaftNode {
        let raft_group_id = (inode % self.total_raft_groups as u64) as u16;
        assert!(node_contains_raft_group(
            self.context.node_index(),
            self.context.total_nodes(),
            raft_group_id,
            self.context.replicas_per_raft_group,
        ));
        &self.groups[&raft_group_id]
    }

    pub fn background_tick(&self) {
        for node in self.groups.values() {
            node.background_tick();
        }
    }
}
