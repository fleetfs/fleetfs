use crate::generated::*;
use crate::peer_client::PeerClient;
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, into_error_code, FlatBufferResponse, ResultResponse};
use flatbuffers::FlatBufferBuilder;
use futures::FutureExt;
use sha2::{Digest, Sha256};
use std::io;
use std::io::Write;
use walkdir::WalkDir;

fn checksum(data_dir: &str) -> io::Result<Vec<u8>> {
    let mut hasher = Sha256::new();
    for entry in WalkDir::new(data_dir).sort_by(|a, b| a.file_name().cmp(b.file_name())) {
        let entry = entry?;
        if entry.file_type().is_file() {
            // TODO hash the data and file attributes too
            let path_bytes = entry
                .path()
                .to_str()
                .unwrap()
                .trim_start_matches(data_dir)
                .as_bytes();
            hasher.write_all(path_bytes).unwrap();
        }
        // TODO handle other file types
    }
    return Ok(hasher.result().to_vec());
}

pub async fn fsck(
    context: LocalContext,
    mut builder: FlatBufferBuilder<'_>,
) -> Result<FlatBufferResponse<'_>, ErrorCode> {
    let checksum = checksum(&context.data_dir).map_err(into_error_code)?;
    let mut peer_futures = vec![];
    for peer in context.peers.iter() {
        let client = PeerClient::new(*peer);
        peer_futures.push(
            client
                .filesystem_checksum()
                .map(|x| x.map_err(into_error_code)),
        );
    }

    futures::future::join_all(peer_futures)
        .map(move |peer_checksums| {
            for peer_checksum in peer_checksums {
                if checksum != peer_checksum? {
                    let args = ErrorResponseArgs {
                        error_code: ErrorCode::Corrupted,
                    };
                    let response_offset =
                        ErrorResponse::create(&mut builder, &args).as_union_value();
                    return Ok((builder, ResponseType::ErrorResponse, response_offset));
                }
            }

            return empty_response(builder);
        })
        .await
}

pub fn checksum_request<'a>(
    local_context: &LocalContext,
    mut builder: FlatBufferBuilder<'a>,
) -> ResultResponse<'a> {
    let checksum = checksum(&local_context.data_dir).map_err(|_| ErrorCode::Uncategorized)?;
    let data_offset = builder.create_vector_direct(&checksum);
    let mut response_builder = ReadResponseBuilder::new(&mut builder);
    response_builder.add_data(data_offset);
    let response_offset = response_builder.finish().as_union_value();

    return Ok((builder, ResponseType::ReadResponse, response_offset));
}
