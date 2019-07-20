use std::error::Error;
use std::fs;

use flatbuffers::FlatBufferBuilder;
use futures::future::Future;
use futures::Stream;
use tokio::codec::length_delimited;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::generated::get_root_as_generic_request;
use crate::handlers::request_router;
use crate::storage::raft_manager::RaftManager;
use crate::utils::node_id_from_address;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::timer::Interval;

#[derive(Clone)]
pub struct LocalContext {
    pub data_dir: String,
    pub peers: Vec<SocketAddr>,
    pub node_id: u64,
}

impl LocalContext {
    pub fn new(data_dir: &str, peers: Vec<SocketAddr>, node_id: u64) -> LocalContext {
        LocalContext {
            data_dir: data_dir.to_string(),
            peers,
            node_id,
        }
    }
}

pub struct Node {
    context: LocalContext,
    raft_manager: RaftManager,
    bind_address: SocketAddr,
}

impl Node {
    pub fn new(node_dir: &str, bind_address: SocketAddr, peers: Vec<SocketAddr>) -> Node {
        let data_dir = Path::new(node_dir).join("data");
        // Unique ID of node within the cluster. Never 0.
        let node_id = node_id_from_address(&bind_address);
        let context = LocalContext::new(data_dir.to_str().unwrap(), peers, node_id);
        Node {
            context: context.clone(),
            raft_manager: RaftManager::new(context.clone()),
            bind_address,
        }
    }

    pub fn run(self) {
        if let Err(why) = fs::create_dir_all(&self.context.data_dir) {
            panic!("Couldn't create storage dir: {}", why.description());
        };

        let listener = TcpListener::bind(&self.bind_address).expect("unable to bind API listener");

        let raft_manager = Arc::new(self.raft_manager);
        let raft_manager_cloned = raft_manager.clone();
        let server = listener
            .incoming()
            .map_err(|e| eprintln!("accept connection failed = {:?}", e))
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let reader = length_delimited::Builder::new()
                    .little_endian()
                    .new_read(reader);

                let cloned_raft = raft_manager.clone();
                let builder = FlatBufferBuilder::new();
                let conn = reader.fold((writer, builder), move |(writer, mut builder), frame| {
                    let request = get_root_as_generic_request(&frame);
                    builder.reset();

                    let response = request_router(request, cloned_raft.clone(), builder);
                    response
                        .map(|response| tokio::io::write_all(writer, response))
                        .flatten()
                        .map(|(writer, written)| (writer, written.into_buffer()))
                });

                tokio::spawn(conn.map(|_| ()).map_err(|_| ()))
            });

        let background_raft = Interval::new(Instant::now(), Duration::from_millis(100))
            .for_each(move |_| {
                raft_manager_cloned.background_tick();
                Ok(())
            })
            .map_err(|e| panic!("Background Raft thread failed error: {:?}", e));

        // TODO: currently we run single threaded to uncover deadlocks more easily
        let mut runtime = tokio::runtime::Builder::new()
            .core_threads(1)
            .build()
            .unwrap();
        runtime.spawn(server);
        runtime.block_on_all(background_raft.map(|_| ())).unwrap();
    }
}
