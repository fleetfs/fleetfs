pub fn node_contains_raft_group(
    node_index: usize,
    total_nodes: usize,
    raft_group_id: u16,
    replicas_per_raft_group: usize,
) -> bool {
    assert_eq!(
        total_nodes % replicas_per_raft_group,
        0,
        "{} % {} != 0",
        total_nodes,
        replicas_per_raft_group
    );

    // Divide all nodes up into groups that can support a raft group
    let node_group = node_index / replicas_per_raft_group;
    let node_groups = total_nodes / replicas_per_raft_group;
    // Round-robin assignment of raft groups to nodes
    let assigned_node_group = raft_group_id % node_groups as u16;

    node_group == assigned_node_group as usize
}
