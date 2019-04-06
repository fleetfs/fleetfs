use hyper::{Body, Client};
use hyper::Method;
use hyper::Request;
use hyper::header::HeaderValue;
use hyper::rt::Future;
use log::info;

use tokio;

use crate::fleetfs::core::{BoxFuture, PATH_HEADER, NO_FORWARD_HEADER};


pub struct PeerClient {
    server_url: String,
}

impl PeerClient {
    pub fn new(server_url: &String) -> PeerClient {
        PeerClient {
            server_url: server_url.clone()
        }
    }

    pub fn truncate(self, filename: &String, new_length: u64) {
        let client = Client::new();
        let uri: hyper::Uri = format!("{}/truncate/{}", self.server_url, new_length).parse().unwrap();
        let mut req = Request::new(Body::from(""));
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

    pub fn unlink(self, filename: String) -> BoxFuture {
        let client = Client::new();
        let uri: hyper::Uri = format!("{}/", self.server_url).parse().unwrap();
        let mut req = Request::new(Body::from(""));
        req.headers_mut().insert(PATH_HEADER, HeaderValue::from_str(filename.as_str()).unwrap());
        req.headers_mut().insert(NO_FORWARD_HEADER, HeaderValue::from_static("true"));
        *req.method_mut() = Method::DELETE;
        *req.uri_mut() = uri.clone();

        let task = client
            .request(req)
            .map(|res| {
                info!("unlink() response: {}", res.status());
                res
            })
            .map_err(|err| {
                info!("unlink() error: {}", err);
                err
            });

        return Box::new(task);
    }
}
