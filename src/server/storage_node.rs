use std::fs;

use flatbuffers::FlatBufferBuilder;
use futures::future::{lazy, ready};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_util::codec::length_delimited;

use log::{debug, error};

use crate::base::utils::node_id_from_address;
use crate::generated::get_root_as_generic_request;
use crate::server::router::request_router;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::client::RemoteRaftGroups;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;

#[derive(Clone)]
pub struct LocalContext {
    pub data_dir: String,
    pub server_ip_port: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub node_id: u64,
    pub replicas_per_raft_group: usize,
}

impl LocalContext {
    pub fn new(
        data_dir: &str,
        server_ip_port: SocketAddr,
        peers: Vec<SocketAddr>,
        node_id: u64,
        replicas_per_raft_group: usize,
    ) -> LocalContext {
        LocalContext {
            data_dir: data_dir.to_string(),
            server_ip_port,
            peers,
            node_id,
            replicas_per_raft_group,
        }
    }

    pub fn total_nodes(&self) -> usize {
        self.peers.len() + 1
    }

    // Index of this node in the cluster. Indices are zero-based and consecutive
    pub fn node_index(&self) -> usize {
        let mut ids = vec![self.node_id];
        for peer in self.peers.iter() {
            ids.push(node_id_from_address(peer));
        }
        ids.sort();

        ids.iter().position(|x| *x == self.node_id).unwrap()
    }

    pub fn peers_with_node_indices(&self) -> Vec<(SocketAddr, usize)> {
        let mut ids = vec![self.node_id];
        for peer in self.peers.iter() {
            ids.push(node_id_from_address(peer));
        }
        ids.sort();

        self.peers
            .iter()
            .map(|peer| {
                let peer_id = node_id_from_address(peer);
                let index = ids.iter().position(|x| *x == peer_id).unwrap();
                (*peer, index)
            })
            .collect()
    }
}

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

        let mut builder = FlatBufferBuilder::new();
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
            builder.reset();
            let request = get_root_as_generic_request(&frame);
            let response = request_router(
                request,
                raft.clone(),
                remote_raft.clone(),
                context.clone(),
                builder,
            )
            .await;
            if let Err(e) = writer.write_all(response.as_ref()).await {
                debug!("Client connection closed: {}", e);
                return;
            };
            builder = response.into_buffer();
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
            panic!("Couldn't create storage dir: {}", why.to_string());
        };

        let bind_address = self.bind_address;
        let context = self.context;

        let raft_manager = Arc::new(self.raft_manager);
        let remote_rafts = Arc::new(self.remote_rafts);
        let raft_manager_cloned = raft_manager.clone();
        let server = async move {
            let mut listener = match TcpListener::bind(bind_address).await {
                Ok(x) => x,
                Err(e) => {
                    error!("Error binding listener: {}", e);
                    return;
                }
            };
            let mut sockets = listener.incoming();
            loop {
                let socket = match sockets.next().await {
                    None => return,
                    Some(connection) => match connection {
                        Ok(x) => x,
                        Err(e) => {
                            debug!("Client error on connect: {}", e);
                            continue;
                        }
                    },
                };
                spawn_connection_handler(
                    socket,
                    raft_manager.clone(),
                    remote_rafts.clone(),
                    context.clone(),
                );
            }
        };

        let background_raft = lazy(|_| {
            tokio::time::interval(Duration::from_millis(100)).for_each(move |_| {
                raft_manager_cloned.background_tick();
                ready(())
            })
        })
        .flatten();

        // TODO: currently we run single threaded to uncover deadlocks more easily
        let mut runtime = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .enable_io()
            .enable_time()
            .core_threads(1)
            .build()
            .unwrap();
        runtime.spawn(server);
        runtime.block_on(background_raft);
    }
}
