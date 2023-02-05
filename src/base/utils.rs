use crate::base::message_types::{ArchivedRkyvGenericResponse, RkyvGenericResponse};
use crate::base::ErrorCode;
use rkyv::AlignedVec;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};

pub fn response_or_error(buffer: &AlignedVec) -> Result<&ArchivedRkyvGenericResponse, ErrorCode> {
    let response = rkyv::check_archived_root::<RkyvGenericResponse>(buffer).unwrap();
    if let Some(error_code) = response.as_error_response() {
        Err(error_code)
    } else {
        Ok(response)
    }
}

pub fn node_contains_raft_group(
    node_index: usize,
    total_nodes: usize,
    raft_group_id: u16,
    replicas_per_raft_group: usize,
) -> bool {
    assert_eq!(
        total_nodes % replicas_per_raft_group,
        0,
        "{total_nodes} % {replicas_per_raft_group} != 0",
    );

    // Divide all nodes up into groups that can support a raft group
    let node_group = node_index / replicas_per_raft_group;
    let node_groups = total_nodes / replicas_per_raft_group;
    // Round-robin assignment of raft groups to nodes
    let assigned_node_group = raft_group_id % node_groups as u16;

    node_group == assigned_node_group as usize
}

pub fn check_access(
    file_uid: u32,
    file_gid: u32,
    file_mode: u16,
    uid: u32,
    gid: u32,
    mut access_mask: i32,
) -> bool {
    // F_OK tests for existence of file
    if access_mask == libc::F_OK {
        return true;
    }
    let file_mode = i32::from(file_mode);

    // root is allowed to read & write anything
    if uid == 0 {
        // root only allowed to exec if one of the X bits is set
        access_mask &= libc::X_OK;
        access_mask -= access_mask & (file_mode >> 6);
        access_mask -= access_mask & (file_mode >> 3);
        access_mask -= access_mask & file_mode;
        return access_mask == 0;
    }

    if uid == file_uid {
        access_mask -= access_mask & (file_mode >> 6);
    } else if gid == file_gid {
        access_mask -= access_mask & (file_mode >> 3);
    } else {
        access_mask -= access_mask & file_mode;
    }

    access_mask == 0
}

pub fn node_id_from_address(address: &SocketAddr) -> u64 {
    let port = address.port();
    match address.ip() {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            u64::from(octets[0]) << 40
                | u64::from(octets[1]) << 32
                | u64::from(octets[2]) << 24
                | u64::from(octets[3]) << 16
                | u64::from(port)
        }
        IpAddr::V6(v6) => {
            // TODO: there could be collisions. Should be generated randomly and then dynamically discovered
            let mut hasher = DefaultHasher::new();
            v6.hash(&mut hasher);
            port.hash(&mut hasher);
            hasher.finish()
        }
    }
}
