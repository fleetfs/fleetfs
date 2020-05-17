use crate::generated::*;
use crate::peer_client::PeerClient;
use crate::storage::lock_table::accessed_inode;
use crate::storage::raft_node::{node_contains_raft_group, RaftNode};
use crate::storage_node::LocalContext;
use crate::utils::LengthPrefixedVec;
use std::collections::HashMap;
use std::future::Future;

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
            groups.insert(i, RaftNode::new(context.clone(), i, rgroups));
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

pub struct RemoteRaftGroups {
    total_raft_groups: u16,
    groups: HashMap<u16, PeerClient>,
}

impl RemoteRaftGroups {
    pub fn new(rgroups: u16, context: LocalContext) -> RemoteRaftGroups {
        let mut groups = HashMap::new();

        for group in 0..rgroups {
            // TODO: this sends all traffic to one node that supports the raft. We should load
            // balance it.
            let addr = context
                .peers_with_node_indices()
                .iter()
                .find_map(|(addr, index)| {
                    if node_contains_raft_group(
                        *index,
                        context.total_nodes(),
                        group,
                        context.replicas_per_raft_group,
                    ) {
                        return Some(*addr);
                    }
                    None
                })
                .unwrap();

            groups.insert(group, PeerClient::new(addr));
        }

        RemoteRaftGroups {
            total_raft_groups: rgroups,
            groups,
        }
    }

    pub fn get_total_raft_groups(&self) -> u16 {
        self.total_raft_groups
    }

    pub fn propose(
        &self,
        inode: u64,
        request: &GenericRequest<'_>,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        let raft_group_id = inode % self.total_raft_groups as u64;
        self.groups
            .get(&(raft_group_id as u16))
            .unwrap()
            // TODO: is accessing _tab.buf safe?
            .send_unprefixed_and_receive_length_prefixed(request._tab.buf.to_vec())
    }

    pub fn propose_to_specific_group(
        &self,
        raft_group: u16,
        request: &GenericRequest<'_>,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        self.groups
            .get(&raft_group)
            .unwrap()
            // TODO: is accessing _tab.buf safe?
            .send_unprefixed_and_receive_length_prefixed(request._tab.buf.to_vec())
    }

    pub fn forward_request(
        &self,
        request: &GenericRequest<'_>,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        let raft_group_id = accessed_inode(request).unwrap() % self.total_raft_groups as u64;
        self.groups
            .get(&(raft_group_id as u16))
            .unwrap()
            // TODO: is accessing _tab.buf safe?
            .send_unprefixed_and_receive_length_prefixed(request._tab.buf.to_vec())
    }
}
