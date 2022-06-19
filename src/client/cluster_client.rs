use crate::base::accessed_inode;
use crate::base::message_types::{ArchivedRkyvRequest, RkyvRequest};
use crate::base::node_contains_raft_group;
use crate::base::LengthPrefixedVec;
use crate::base::LocalContext;
use crate::client::{PeerClient, TcpPeerClient};
use crate::generated::*;
use futures_util::future::FutureExt;
use rkyv::Deserialize;
use std::collections::HashMap;
use std::future::Future;

pub struct RemoteRaftGroups {
    total_raft_groups: u16,
    groups: HashMap<u16, TcpPeerClient>,
}

impl RemoteRaftGroups {
    pub fn new(rgroups: u16, context: LocalContext) -> RemoteRaftGroups {
        let mut groups = HashMap::new();

        for group in 0..rgroups {
            let addr = if node_contains_raft_group(
                context.node_index(),
                context.total_nodes(),
                group,
                context.replicas_per_raft_group,
            ) {
                context.server_ip_port
            } else {
                // TODO: this sends all traffic to one node that supports the raft. We should load
                // balance it.
                context
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
                    .unwrap()
            };

            groups.insert(group, TcpPeerClient::new(addr));
        }

        RemoteRaftGroups {
            total_raft_groups: rgroups,
            groups,
        }
    }

    pub fn get_total_raft_groups(&self) -> u16 {
        self.total_raft_groups
    }

    pub fn wait_for_ready(&self) -> impl Future<Output = Result<(), std::io::Error>> {
        let mut group_futures = vec![];
        for (group, client) in self.groups.iter() {
            let group_future = client.send(&RkyvRequest::RaftGroupLeader { raft_group: *group });
            group_futures.push(group_future);
        }

        futures::future::join_all(group_futures).map(|results| {
            for result in results {
                result?;
            }

            Ok(())
        })
    }

    pub fn propose(
        &self,
        inode: u64,
        request: &RkyvRequest,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        let raft_group_id = inode % self.total_raft_groups as u64;
        self.groups
            .get(&(raft_group_id as u16))
            .unwrap()
            .send(request)
    }

    pub fn propose_flatbuffer(
        &self,
        inode: u64,
        request: &GenericRequest<'_>,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        let raft_group_id = inode % self.total_raft_groups as u64;
        self.groups
            .get(&(raft_group_id as u16))
            .unwrap()
            // TODO: is accessing _tab.buf safe?
            .send_flatbuffer_unprefixed_and_receive_length_prefixed(request._tab.buf.to_vec())
    }

    pub fn propose_to_specific_group(
        &self,
        raft_group: u16,
        request: &RkyvRequest,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        self.groups.get(&raft_group).unwrap().send(request)
    }

    pub fn forward_flatbuffer_request(
        &self,
        request: &GenericRequest<'_>,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        let raft_group_id = accessed_inode(request).unwrap() % self.total_raft_groups as u64;
        self.groups
            .get(&(raft_group_id as u16))
            .unwrap()
            // TODO: is accessing _tab.buf safe?
            .send_flatbuffer_unprefixed_and_receive_length_prefixed(request._tab.buf.to_vec())
    }

    pub fn forward_archived_request(
        &self,
        request: &ArchivedRkyvRequest,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        // TODO: avoid this deserializing and re-serializing. Unfortunately, I can't figure out
        // how to get the underlying bytes out of the archived type
        let deserialized: RkyvRequest = request.deserialize(&mut rkyv::Infallible).unwrap();
        self.forward_request(&deserialized)
    }

    pub fn forward_request(
        &self,
        request: &RkyvRequest,
    ) -> impl Future<Output = Result<LengthPrefixedVec, std::io::Error>> {
        let rkyv_bytes = rkyv::to_bytes::<_, 64>(request).unwrap();
        let serialized = rkyv::check_archived_root::<RkyvRequest>(&rkyv_bytes).unwrap();
        let raft_group_id = serialized.meta_info().inode.unwrap() % self.total_raft_groups as u64;
        self.groups
            .get(&(raft_group_id as u16))
            .unwrap()
            .send_unprefixed_and_receive_length_prefixed(rkyv_bytes)
    }
}
