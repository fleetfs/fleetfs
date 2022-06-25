use log::error;
use std::net::SocketAddr;

use crate::base::message_types::{CommitId, ErrorCode, RkyvRequest};
use crate::base::response_or_error;
use byteorder::{ByteOrder, LittleEndian};
use futures::future::{ok, ready, BoxFuture, Either};
use futures::FutureExt;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use rkyv::AlignedVec;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// TODO: should have a larger pool for connections to the leader, and smaller for other peers
const POOL_SIZE: usize = 8;

pub trait PeerClient {
    fn send_raw<T: AsRef<[u8]> + Send + 'static>(
        &self,
        data: T,
    ) -> BoxFuture<'static, Result<AlignedVec, std::io::Error>>;

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

async fn async_send_and_receive<T: AsRef<[u8]> + Send>(
    mut stream: TcpStream,
    data: T,
    pool: Arc<Mutex<Vec<TcpStream>>>,
) -> Result<AlignedVec, std::io::Error> {
    let mut request = vec![0; 4 + data.as_ref().len()];
    LittleEndian::write_u32(&mut request[..4], data.as_ref().len() as u32);
    // TODO: remove this copy and use vectored write of the header and data separately,
    // once that's supported in tokio: https://github.com/tokio-rs/tokio/issues/1271
    // We merge them into a single buffer to be sure it's sent a single packet.
    // Otherwise delayed TCP ACKs can add ~40ms of latency: https://eklitzke.org/the-caveats-of-tcp-nodelay
    request[4..(data.as_ref().len() + 4)].clone_from_slice(data.as_ref());
    stream.write_all(&request).await?;

    let mut response_size = vec![0; 4];
    stream.read_exact(&mut response_size).await?;

    let size = LittleEndian::read_u32(&response_size);
    let mut buffer = AlignedVec::with_capacity(size as usize);
    // TODO: this seems very wasteful. There should be a AlignedVec.zeros() method
    buffer.extend_from_slice(&vec![0u8; size as usize]);
    stream.read_exact(&mut buffer).await?;

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

    pub fn send(
        &self,
        request: &RkyvRequest,
    ) -> BoxFuture<'static, Result<AlignedVec, std::io::Error>> {
        let pool = self.pool.clone();
        let rkyv_bytes = rkyv::to_bytes::<_, 64>(request).unwrap();

        self.connect()
            .then(move |tcp_stream| match tcp_stream {
                Ok(stream) => Either::Left(async_send_and_receive(stream, rkyv_bytes, pool)),
                Err(e) => Either::Right(ready(Err(e))),
            })
            .boxed()
    }

    fn return_connection(pool: Arc<Mutex<Vec<TcpStream>>>, connection: TcpStream) {
        let mut locked = pool.lock().unwrap();
        if locked.len() < POOL_SIZE {
            locked.push(connection);
        }
    }
}

impl PeerClient for TcpPeerClient {
    fn send_raw<T: AsRef<[u8]> + Send + 'static>(
        &self,
        data: T,
    ) -> BoxFuture<'static, Result<AlignedVec, std::io::Error>> {
        let pool = self.pool.clone();

        self.connect()
            .then(move |tcp_stream| match tcp_stream {
                Ok(stream) => Either::Left(async_send_and_receive(stream, data, pool)),
                Err(e) => Either::Right(ready(Err(e))),
            })
            .boxed()
    }

    fn send_raft_message(&self, raft_group: u16, message: Message) -> BoxFuture<'static, ()> {
        let serialized_message = message.write_to_bytes().unwrap();
        let ip_and_port = self.server_ip_port;
        self.send(&RkyvRequest::RaftMessage {
            raft_group,
            data: serialized_message,
        })
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
        self.send(&RkyvRequest::LatestCommit { raft_group })
            .map(|response| {
                response.map(|data| {
                    let response = response_or_error(&data).unwrap();
                    response.as_latest_commit_response().unwrap().1
                })
            })
            .boxed()
    }

    fn filesystem_checksum(&self) -> BoxFuture<'static, Result<HashMap<u16, Vec<u8>>, ErrorCode>> {
        self.send(&RkyvRequest::FilesystemChecksum)
            .map(|maybe_response| {
                let data = maybe_response.map_err(|_| ErrorCode::Uncategorized)?;
                let response = response_or_error(&data).unwrap();
                let checksums = response.as_checksum_response().unwrap();

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
        let request = RkyvRequest::ReadRaw {
            required_commit,
            inode,
            offset,
            read_size: size,
        };

        self.send(&request)
            .map(|response| {
                let response = response?;
                let response = response_or_error(&response).unwrap();
                let data = response.as_read_response().unwrap().to_vec();
                Ok(data)
            })
            .boxed()
    }
}
