use crate::base::utils::{empty_response, FlatBufferResponse, ResultResponse};
use crate::generated::*;
use crate::peer_client::PeerClient;
use crate::server::storage_node::LocalContext;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::storage::raft_node::sync_with_leader;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn fsck(
    context: LocalContext,
    raft: Arc<LocalRaftGroupManager>,
    builder: FlatBufferBuilder<'_>,
) -> Result<FlatBufferResponse<'_>, ErrorCode> {
    let mut local_checksums = HashMap::new();
    for rgroup in raft.all_groups() {
        sync_with_leader(rgroup).await?;
        let checksum = rgroup.local_data_checksum()?;
        local_checksums.insert(rgroup.get_raft_group_id(), checksum);
    }

    let mut peer_futures = vec![];
    for peer in context.peers.iter() {
        let client = PeerClient::new(*peer);
        peer_futures.push(client.filesystem_checksum());
    }

    futures::future::join_all(peer_futures)
        .map(move |peer_checksums| {
            let mut all_checksums = local_checksums;
            for peer_rgroup_checksums in peer_checksums {
                for (rgroup_id, checksum) in peer_rgroup_checksums? {
                    if *all_checksums
                        .entry(rgroup_id)
                        .or_insert_with(|| checksum.clone())
                        != checksum
                    {
                        return Err(ErrorCode::Corrupted);
                    }
                }
            }

            return empty_response(builder);
        })
        .await
}

pub async fn checksum_request(
    raft: Arc<LocalRaftGroupManager>,
    mut builder: FlatBufferBuilder<'_>,
) -> ResultResponse<'_> {
    let mut checksums = vec![];
    for rgroup in raft.all_groups() {
        sync_with_leader(rgroup).await?;

        let checksum = rgroup.local_data_checksum()?;
        let checksum_offset = builder.create_vector_direct(&checksum);
        let group_checksum = GroupChecksum::create(
            &mut builder,
            &GroupChecksumArgs {
                raft_group: rgroup.get_raft_group_id(),
                checksum: Some(checksum_offset),
            },
        );
        checksums.push(group_checksum);
    }
    builder.start_vector::<WIPOffset<GroupChecksum>>(checksums.len());
    for &checksum in checksums.iter() {
        builder.push(checksum);
    }
    let checksums_offset = builder.end_vector(checksums.len());
    let mut response_builder = ChecksumResponseBuilder::new(&mut builder);
    response_builder.add_checksums(checksums_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::ChecksumResponse, response_offset));
}
