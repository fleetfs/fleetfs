use std::fs;

use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::length_delimited;

use log::{debug, error};

use crate::base::node_id_from_address;
use crate::storage::message_handlers::request_router;
use byteorder::{ByteOrder, LittleEndian};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::base::LocalContext;
use crate::client::RemoteRaftGroups;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use futures_util::stream::StreamExt;
use rkyv::AlignedVec;
use tokio::io::AsyncWriteExt;

fn spawn_connection_handler(
    mut socket: TcpStream,
    raft: Arc<LocalRaftGroupManager>,
    remote_raft: Arc<RemoteRaftGroups>,
    context: LocalContext,
) {
    tokio::spawn(async move {
        let (reader, mut writer) = socket.split();
        let mut reader = length_delimited::Builder::new()
            .little_endian()
            .new_read(reader);

        loop {
            let frame = match reader.next().await {
                None => return,
                Some(bytes) => match bytes {
                    Ok(x) => x,
                    Err(e) => {
                        debug!("Client connection closed: {}", e);
                        return;
                    }
                },
            };
            let mut aligned = AlignedVec::with_capacity(frame.len());
            aligned.extend_from_slice(&frame);
            let response =
                request_router(aligned, raft.clone(), remote_raft.clone(), context.clone()).await;
            // TODO optimize this to avoid the copy
            let mut result = vec![0u8; response.len() + 4];
            result[4..].copy_from_slice(&response);
            LittleEndian::write_u32(&mut result, response.len() as u32);
            if let Err(e) = writer.write_all(&result).await {
                debug!("Client connection closed: {}", e);
                return;
            };
        }
    });
}

pub struct Node {
    context: LocalContext,
    raft_manager: LocalRaftGroupManager,
    remote_rafts: RemoteRaftGroups,
    bind_address: SocketAddr,
}

impl Node {
    pub fn new(
        node_dir: &str,
        bind_address: SocketAddr,
        peers: Vec<SocketAddr>,
        replicas_per_raft_group: usize,
    ) -> Node {
        let data_dir = Path::new(node_dir).join("data");
        #[allow(clippy::expect_fun_call)]
        fs::create_dir_all(&data_dir)
            .expect(&format!("Failed to create data dir: {:?}", &data_dir));
        // Unique ID of node within the cluster. Never 0.
        let node_id = node_id_from_address(&bind_address);
        let context = LocalContext::new(
            data_dir.to_str().unwrap(),
            bind_address,
            peers,
            node_id,
            replicas_per_raft_group,
        );
        // TODO: Make auto adjust based on load. Using same number as nodes is just a heuristic, so that
        // it scales with the cluster
        let rgroups = context.total_nodes() as u16;
        Node {
            context: context.clone(),
            raft_manager: LocalRaftGroupManager::new(rgroups, context.clone()),
            remote_rafts: RemoteRaftGroups::new(rgroups, context),
            bind_address,
        }
    }

    pub fn run(self) {
        if let Err(why) = fs::create_dir_all(&self.context.data_dir) {
            panic!("Couldn't create storage dir: {}", why);
        };

        let bind_address = self.bind_address;
        let context = self.context;

        let raft_manager = Arc::new(self.raft_manager);
        let remote_rafts = Arc::new(self.remote_rafts);
        let raft_manager_cloned = raft_manager.clone();
        let server = async move {
            let listener = match TcpListener::bind(bind_address).await {
                Ok(x) => x,
                Err(e) => {
                    error!("Error binding listener: {}", e);
                    return;
                }
            };
            loop {
                let socket = match listener.accept().await {
                    Ok((sock, _)) => sock,
                    Err(e) => {
                        debug!("Client error on connect: {}", e);
                        continue;
                    }
                };
                spawn_connection_handler(
                    socket,
                    raft_manager.clone(),
                    remote_rafts.clone(),
                    context.clone(),
                );
            }
        };

        let background_raft = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                raft_manager_cloned.background_tick();
            }
        };

        // TODO: currently we run single threaded to uncover deadlocks more easily
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .worker_threads(1)
            .build()
            .unwrap();
        runtime.spawn(server);
        runtime.block_on(background_raft);
    }
}
