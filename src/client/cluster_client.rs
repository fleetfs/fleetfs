use crate::base::message_utils::accessed_inode;
use crate::base::utils::{finalize_request_without_prefix, LengthPrefixedVec};
use crate::client::PeerClient;
use crate::generated::*;
use crate::server::LocalContext;
use crate::storage::raft_node::node_contains_raft_group;
use flatbuffers::FlatBufferBuilder;
use futures_util::future::FutureExt;
use std::collections::HashMap;
use std::future::Future;

pub struct RemoteRaftGroups {
    total_raft_groups: u16,
    groups: HashMap<u16, PeerClient>,
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

    pub fn wait_for_ready(&self) -> impl Future<Output = Result<(), std::io::Error>> {
        let mut group_futures = vec![];
        for (group, client) in self.groups.iter() {
            let mut builder = FlatBufferBuilder::new();
            let mut request_builder = RaftGroupLeaderRequestBuilder::new(&mut builder);
            request_builder.add_raft_group(*group);
            let finish_offset = request_builder.finish().as_union_value();
            // Don't need length prefix, since we're not serializing over network
            finalize_request_without_prefix(
                &mut builder,
                RequestType::RaftGroupLeaderRequest,
                finish_offset,
            );

            let group_future = client
                .send_unprefixed_and_receive_length_prefixed(builder.finished_data().to_vec());

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
