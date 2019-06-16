use std::error::Error;
use std::fs;

use flatbuffers::FlatBufferBuilder;
use futures::Stream;
use tokio::codec::length_delimited;
use tokio::net::TcpListener;
use tokio::prelude::*;

use crate::handlers::client_request_handler;
use crate::storage::raft_manager::RaftManager;
use crate::utils::WritableFlatBuffer;
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
    port: u16,
}

impl Node {
    pub fn new(node_dir: &str, port: u16, peers: Vec<SocketAddr>) -> Node {
        let data_dir = Path::new(node_dir).join("data");
        // TODO huge hack. Should be generated randomly and then dynamically discovered
        // Unique ID of node within the cluster. Never 0.
        let node_id = u64::from(port);
        let context = LocalContext::new(data_dir.to_str().unwrap(), peers, node_id);
        Node {
            context: context.clone(),
            raft_manager: RaftManager::new(context.clone()),
            port,
        }
    }

    pub fn run(self) {
        if let Err(why) = fs::create_dir_all(&self.context.data_dir) {
            panic!("Couldn't create storage dir: {}", why.description());
        };

        let address = ([127, 0, 0, 1], self.port).into();
        let listener = TcpListener::bind(&address).expect("unable to bind API listener");

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
                let conn = reader.fold((writer, builder), move |(writer, builder), frame| {
                    let response = client_request_handler(builder, frame, cloned_raft.clone());
                    response
                        .map(|builder| {
                            let writable = WritableFlatBuffer::new(builder);
                            tokio::io::write_all(writer, writable)
                        })
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
