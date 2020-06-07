use crate::base::utils::node_id_from_address;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct LocalContext {
    pub data_dir: String,
    pub server_ip_port: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub node_id: u64,
    pub replicas_per_raft_group: usize,
}

impl LocalContext {
    pub fn new(
        data_dir: &str,
        server_ip_port: SocketAddr,
        peers: Vec<SocketAddr>,
        node_id: u64,
        replicas_per_raft_group: usize,
    ) -> LocalContext {
        LocalContext {
            data_dir: data_dir.to_string(),
            server_ip_port,
            peers,
            node_id,
            replicas_per_raft_group,
        }
    }

    pub fn total_nodes(&self) -> usize {
        self.peers.len() + 1
    }

    // Index of this node in the cluster. Indices are zero-based and consecutive
    pub fn node_index(&self) -> usize {
        let mut ids = vec![self.node_id];
        for peer in self.peers.iter() {
            ids.push(node_id_from_address(peer));
        }
        ids.sort();

        ids.iter().position(|x| *x == self.node_id).unwrap()
    }

    pub fn peers_with_node_indices(&self) -> Vec<(SocketAddr, usize)> {
        let mut ids = vec![self.node_id];
        for peer in self.peers.iter() {
            ids.push(node_id_from_address(peer));
        }
        ids.sort();

        self.peers
            .iter()
            .map(|peer| {
                let peer_id = node_id_from_address(peer);
                let index = ids.iter().position(|x| *x == peer_id).unwrap();
                (*peer, index)
            })
            .collect()
    }
}
