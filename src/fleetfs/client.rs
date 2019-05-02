use hyper::{Body, Client};
use hyper::Method;
use hyper::Request;
use hyper::header::HeaderValue;
use hyper::rt::Future;
use log::info;

use tokio;

use crate::fleetfs::core::{PATH_HEADER, NO_FORWARD_HEADER};
// TODO: should move this somewhere else
use crate::fleetfs::fuse::NodeClient;
use std::net::SocketAddr;


pub struct PeerClient {
    server_url: String,
    node_client: NodeClient
}

impl PeerClient {
    pub fn new(server_url: &String, server_ip_and_port: SocketAddr) -> PeerClient {
        PeerClient {
            server_url: server_url.clone(),
            node_client: NodeClient::new(server_url, &server_ip_and_port)
        }
    }

    pub fn hardlink(&self, path: &String, new_path: &String) {
        self.node_client.hardlink(path, new_path, false).unwrap();
    }

    pub fn rename(&self, path: &String, new_path: &String) {
        self.node_client.rename(path, new_path, false).unwrap();
    }

    pub fn utimens(&self, path: &String, atime_secs: i64, atime_nanos: i32, mtime_secs: i64, mtime_nanos: i32) {
        self.node_client.utimens(path, atime_secs, atime_nanos, mtime_secs, mtime_nanos, false).unwrap();
    }

    pub fn chmod(&self, path: &String, mode: u32) {
        self.node_client.chmod(path, mode, false).unwrap();
    }

    pub fn truncate(self, filename: &String, new_length: u64) {
        self.node_client.truncate(filename, new_length, false).unwrap();
    }

    pub fn write(self, filename: String, offset: u64, bytes: &[u8]) {
        let client = Client::new();
        let uri: hyper::Uri = format!("{}/{}", self.server_url, offset).parse().unwrap();
        let mut req = Request::new(Body::from(Vec::from(bytes)));
        req.headers_mut().insert(PATH_HEADER, HeaderValue::from_str(filename.as_str()).unwrap());
        req.headers_mut().insert(NO_FORWARD_HEADER, HeaderValue::from_static("true"));
        *req.method_mut() = Method::POST;
        *req.uri_mut() = uri.clone();

        let task = client
            .request(req)
            .map(|res| {
                info!("write() response: {}", res.status());
            })
            .map_err(|err| {
                info!("write() error: {}", err);
            });

        tokio::spawn(task);
    }

    pub fn unlink(self, filename: &String) {
        self.node_client.unlink(filename, false).unwrap();
    }
}
