use crate::base::message_types::{ErrorCode, RkyvGenericResponse};
use crate::base::LocalContext;
use crate::base::{empty_response, FlatBufferResponse, ResultResponse};
use crate::client::{PeerClient, TcpPeerClient};
use crate::generated::*;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::storage::raft_node::sync_with_leader;
use flatbuffers::FlatBufferBuilder;
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
        let client = TcpPeerClient::new(*peer);
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
    let mut checksums = HashMap::new();
    for rgroup in raft.all_groups() {
        sync_with_leader(rgroup).await?;

        let checksum = rgroup.local_data_checksum()?;
        checksums.insert(rgroup.get_raft_group_id(), checksum);
    }
    let rkyv_response = RkyvGenericResponse::Checksums(checksums);
    let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
    let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
    let mut response_builder = RkyvResponseBuilder::new(&mut builder);
    response_builder.add_rkyv_data(flatbuffer_offset);
    let offset = response_builder.finish().as_union_value();
    return Ok((builder, ResponseType::RkyvResponse, offset));
}
