use log::error;
use std::net::SocketAddr;

use flatbuffers::FlatBufferBuilder;

use crate::base::{finalize_request, response_or_error, FlatBufferWithResponse, LengthPrefixedVec};
use crate::generated::*;
use byteorder::{ByteOrder, LittleEndian};
use futures::future::{ok, ready, BoxFuture, Either};
use futures::FutureExt;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// TODO: should have a larger pool for connections to the leader, and smaller for other peers
const POOL_SIZE: usize = 8;

pub trait PeerClient {
    fn send_and_receive_length_prefixed<T: AsRef<[u8]> + Send + Sync + 'static>(
        &self,
        data: T,
    ) -> BoxFuture<'static, Result<Vec<u8>, std::io::Error>>;

    fn send_unprefixed_and_receive_length_prefixed<T: AsRef<[u8]> + Send + 'static>(
        &self,
        data: T,
    ) -> BoxFuture<'static, Result<LengthPrefixedVec, std::io::Error>>;

    fn send_raft_message(&self, raft_group: u16, message: Message) -> BoxFuture<'static, ()>;

    fn get_latest_commit(&self, raft_group: u16)
        -> BoxFuture<'static, Result<u64, std::io::Error>>;

    fn filesystem_checksum(&self) -> BoxFuture<'static, Result<HashMap<u16, Vec<u8>>, ErrorCode>>;

    fn read_raw(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        required_commit: CommitId,
    ) -> BoxFuture<'static, Result<Vec<u8>, std::io::Error>>;
}

#[derive(Debug)]
pub struct TcpPeerClient {
    server_ip_port: SocketAddr,
    pool: Arc<Mutex<Vec<TcpStream>>>,
}

async fn async_send_receive_length_prefixed<T: AsRef<[u8]> + Send>(
    mut stream: TcpStream,
    data: T,
    pool: Arc<Mutex<Vec<TcpStream>>>,
) -> Result<Vec<u8>, std::io::Error> {
    stream.write_all(data.as_ref()).await?;

    let mut response_size = vec![0; 4];
    stream.read_exact(&mut response_size).await?;

    let size = LittleEndian::read_u32(&response_size);
    let mut buffer = vec![0; size as usize];
    stream.read_exact(&mut buffer).await?;

    TcpPeerClient::return_connection(pool, stream);

    Ok(buffer)
}

async fn async_send_unprefixed_receive_length_prefixed<T: AsRef<[u8]> + Send>(
    mut stream: TcpStream,
    data: T,
    pool: Arc<Mutex<Vec<TcpStream>>>,
) -> Result<LengthPrefixedVec, std::io::Error> {
    let mut request = vec![0; 4 + data.as_ref().len()];
    LittleEndian::write_u32(&mut request[..4], data.as_ref().len() as u32);
    // TODO: remove this copy and use vectored write of the header and data separately,
    // once that's supported in tokio: https://github.com/tokio-rs/tokio/issues/1271
    // We merge them into a single buffer to be sure it's sent a single packet.
    // Otherwise delayed TCP ACKs can add ~40ms of latency: https://eklitzke.org/the-caveats-of-tcp-nodelay
    request[4..(data.as_ref().len() + 4)].clone_from_slice(&data.as_ref()[..]);
    stream.write_all(&request).await?;

    let mut response_size = vec![0; 4];
    stream.read_exact(&mut response_size).await?;

    let size = LittleEndian::read_u32(&response_size);
    let mut buffer = LengthPrefixedVec::zeros(size as usize);
    stream.read_exact(buffer.bytes_mut()).await?;

    TcpPeerClient::return_connection(pool, stream);

    Ok(buffer)
}

impl TcpPeerClient {
    pub fn new(server_ip_port: SocketAddr) -> TcpPeerClient {
        TcpPeerClient {
            server_ip_port,
            pool: Arc::new(Mutex::new(vec![])),
        }
    }

    fn connect(&self) -> BoxFuture<'static, Result<TcpStream, std::io::Error>> {
        let mut locked = self.pool.lock().unwrap();
        if let Some(stream) = locked.pop() {
            ok(stream).boxed()
        } else {
            // TODO: should have an upper limit on the number of outstanding connections
            TcpStream::connect(self.server_ip_port).boxed()
        }
    }

    fn return_connection(pool: Arc<Mutex<Vec<TcpStream>>>, connection: TcpStream) {
        let mut locked = pool.lock().unwrap();
        if locked.len() < POOL_SIZE {
            locked.push(connection);
        }
    }
}

impl PeerClient for TcpPeerClient {
    fn send_and_receive_length_prefixed<T: AsRef<[u8]> + Send + Sync + 'static>(
        &self,
        data: T,
    ) -> BoxFuture<'static, Result<Vec<u8>, std::io::Error>> {
        let pool = self.pool.clone();

        self.connect()
            .then(move |tcp_stream| match tcp_stream {
                Ok(stream) => Either::Left(async_send_receive_length_prefixed(stream, data, pool)),
                Err(e) => Either::Right(ready(Err(e))),
            })
            .boxed()
    }

    fn send_unprefixed_and_receive_length_prefixed<T: AsRef<[u8]> + Send + 'static>(
        &self,
        data: T,
    ) -> BoxFuture<'static, Result<LengthPrefixedVec, std::io::Error>> {
        let pool = self.pool.clone();

        self.connect()
            .then(move |tcp_stream| match tcp_stream {
                Ok(stream) => Either::Left(async_send_unprefixed_receive_length_prefixed(
                    stream, data, pool,
                )),
                Err(e) => Either::Right(ready(Err(e))),
            })
            .boxed()
    }

    fn send_raft_message(&self, raft_group: u16, message: Message) -> BoxFuture<'static, ()> {
        let serialized_message = message.write_to_bytes().unwrap();
        let mut builder = FlatBufferBuilder::new();
        let data_offset = builder.create_vector_direct(&serialized_message);
        let mut request_builder = RaftRequestBuilder::new(&mut builder);
        request_builder.add_raft_group(raft_group);
        request_builder.add_message(data_offset);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::RaftRequest, finish_offset);

        let ip_and_port = self.server_ip_port;
        self.send_and_receive_length_prefixed(builder.finished_data().to_vec())
            .map(move |x| {
                if let Err(io_error) = x {
                    error!(
                        "Error sending Raft message to {}: {}",
                        ip_and_port, io_error
                    );
                }
            })
            .boxed()
    }

    fn get_latest_commit(
        &self,
        raft_group: u16,
    ) -> BoxFuture<'static, Result<u64, std::io::Error>> {
        let mut builder = FlatBufferBuilder::new();
        let mut request_builder = LatestCommitRequestBuilder::new(&mut builder);
        request_builder.add_raft_group(raft_group);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::LatestCommitRequest,
            finish_offset,
        );

        self.send_and_receive_length_prefixed(builder.finished_data().to_vec())
            .map(|response| {
                response.map(|data| {
                    response_or_error(&data)
                        .unwrap()
                        .response_as_latest_commit_response()
                        .unwrap()
                        .index()
                })
            })
            .boxed()
    }

    fn filesystem_checksum(&self) -> BoxFuture<'static, Result<HashMap<u16, Vec<u8>>, ErrorCode>> {
        let mut builder = FlatBufferBuilder::new();
        let request_builder = FilesystemChecksumRequestBuilder::new(&mut builder);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(
            &mut builder,
            RequestType::FilesystemChecksumRequest,
            finish_offset,
        );

        self.send_and_receive_length_prefixed(builder.finished_data().to_vec())
            .map(|maybe_response| {
                let mut checksums = HashMap::new();
                let response = maybe_response.map_err(|_| ErrorCode::Uncategorized)?;
                let checksums_response = response_or_error(&response)?
                    .response_as_checksum_response()
                    .unwrap();

                let entries = checksums_response.checksums();
                for i in 0..entries.len() {
                    let entry = entries.get(i);
                    checksums.insert(entry.raft_group(), entry.checksum().to_vec());
                }
                Ok(checksums)
            })
            .boxed()
    }

    fn read_raw(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        required_commit: CommitId,
    ) -> BoxFuture<'static, Result<Vec<u8>, std::io::Error>> {
        let mut builder = FlatBufferBuilder::new();
        let mut request_builder = ReadRawRequestBuilder::new(&mut builder);
        request_builder.add_offset(offset);
        request_builder.add_read_size(size);
        request_builder.add_inode(inode);
        request_builder.add_required_commit(&required_commit);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request(&mut builder, RequestType::ReadRawRequest, finish_offset);

        self.send_and_receive_length_prefixed(FlatBufferWithResponse::new(builder))
            .map(|response| {
                let mut response = response?;
                let length = response.len();
                assert_eq!(
                    ErrorCode::DefaultValueNotAnError as u8,
                    response[length - 1]
                );
                response.pop();
                Ok(response)
            })
            .boxed()
    }
}
