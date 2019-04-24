use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Seek, Read};
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

use futures::sync::mpsc::channel;

use bytes::{Buf, BytesMut, BufMut};
use futures::future;
use futures::Stream;
use hyper::{Body, Response, Server, StatusCode};
use hyper::Method;
use hyper::Request;
use hyper::rt::Future;
use hyper::service::service_fn;
use log::info;
use crate::fleetfs::client::PeerClient;
use std::collections::HashMap;
use tokio::net::{TcpListener};
use tokio::io::ErrorKind;
use tokio::prelude::*;

use byteorder::LittleEndian;
use byteorder::ByteOrder;
use tokio_io::codec::Decoder;
use tokio_io::_tokio_codec::Encoder;
use tokio::codec::Framed;

pub const PATH_HEADER: &str = "X-FleetFS-Path";
pub const OFFSET_HEADER: &str = "X-FleetFS-Offset";
pub const SIZE_HEADER: &str = "X-FleetFS-Size";
pub const NO_FORWARD_HEADER: &str = "X-FleetFS-No-Forward";


pub type BoxFuture = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

struct DistributedFile {
    filename: String,
    local_data_dir: String,
    peers: Vec<PeerClient>
}

impl DistributedFile {
    fn new(filename: String, local_data_dir: String, peers: &[String]) -> DistributedFile {
        DistributedFile {
            filename,
            local_data_dir,
            peers: peers.into_iter().map(|peer| PeerClient::new(peer)).collect()
        }
    }

    fn truncate(self, req: Request<Body>) -> BoxFuture {
        let new_length: u64 = req.uri().path()["/truncate/".len()..].parse().unwrap();
        let forward: bool = req.headers().get(NO_FORWARD_HEADER).is_none();
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                let path = Path::new(&self.local_data_dir).join(&self.filename);
                let display = path.display();

                let file = match File::create(&path) {
                    Err(why) => panic!("couldn't create {}: {}",
                                       display,
                                       why.description()),
                    Ok(file) => file,
                };
                file.set_len(new_length).unwrap();

                if forward {
                    for peer in self.peers {
                        peer.truncate(&self.filename, new_length);
                    }
                }

                Response::new(Body::from("done"))
            });

        Box::new(response)
    }

    fn write(self, req: Request<Body>) -> BoxFuture {
        let offset: u64 = req.uri().path()[1..].parse().unwrap();
        let forward: bool = req.headers().get(NO_FORWARD_HEADER).is_none();
        let response = req.into_body()
            .concat2()
            .map(move |chunk| {
                let bytes = chunk.bytes();
                let path = Path::new(&self.local_data_dir).join(&self.filename);
                let display = path.display();

                let mut file = match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&path) {
                    Err(why) => panic!("couldn't create {}: {}",
                                       display,
                                       why.description()),
                    Ok(file) => file,
                };

                match file.seek(SeekFrom::Start(offset)) {
                    Err(why) => {
                        panic!("couldn't seek to {} in {} because {}", offset, display,
                               why.description())
                    },
                    Ok(_) => {}
                }

                match file.write_all(bytes) {
                    Err(why) => {
                        panic!("couldn't write to {}: {}", display,
                               why.description())
                    },
                    Ok(_) => info!("successfully wrote to {}", display),
                }

                if forward {
                    for peer in self.peers {
                        peer.write(format!("/{}", &self.filename), offset, bytes);
                    }
                }

                Response::new(Body::from("done"))
            });

        Box::new(response)
    }

    fn list_dir(self, req: Request<Body>) -> BoxFuture {
        assert_eq!(self.filename.len(), 0);
        info!("Listing directory");
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                let mut entries = vec![];
                for entry in fs::read_dir(Path::new(&self.local_data_dir).join(self.filename)).unwrap() {
                    let filename = entry.unwrap().path().clone().to_str().unwrap().to_string();
                    // TODO: there must be a better way to strip the data_dir substring off the left side
                    entries.push(filename.split_at(self.local_data_dir.len() + 1).1.to_string());
                }

                Response::new(Body::from(serde_json::to_string(&entries).unwrap()))
            });

        Box::new(response)
    }

    fn getattr(self, req: Request<Body>) -> BoxFuture {
        let path = Path::new(&self.local_data_dir).join(&self.filename);
        info!("Getting metadata for {:?}", &path);

        let response = req.into_body()
            .concat2()
            .map(move |_| {
                let metadata = fs::metadata(path).unwrap();

                let mut map = HashMap::new();
                map.insert("length", metadata.len());

                Response::new(Body::from(serde_json::to_string(&map).unwrap()))
            });

        Box::new(response)
    }

    fn read_v2(self, offset: u64, size: u32) -> Vec<u8> {
        assert_ne!(self.filename.len(), 0);

        info!("[v2 API] Reading file: {}. data_dir={}", self.filename, self.local_data_dir);
        let path = Path::new(&self.local_data_dir).join(&self.filename);
        let display = path.display();

        let mut file = match File::open(&path) {
            Err(why) => panic!("couldn't create {}: {}",
                               display,
                               why.description()),
            Ok(file) => file,
        };

        match file.seek(SeekFrom::Start(offset)) {
            Err(why) => {
                panic!("couldn't seek to {} in {} because {}", offset, display,
                       why.description())
            },
            Ok(_) => {}
        }

        let mut handle = file.take(size as u64);

        let mut contents = vec![0; size as usize];
        let bytes_read = handle.read(&mut contents);
        match bytes_read {
            Err(_) => panic!(),
            Ok(size) => contents.truncate(size)
        };

        return contents;
    }

    fn unlink(self, req: Request<Body>) -> BoxFuture {
        let forward: bool = req.headers().get(NO_FORWARD_HEADER).is_none();
        assert_ne!(self.filename.len(), 0);

        info!("Deleting file");
        let path = Path::new(&self.local_data_dir).join(&self.filename);
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                fs::remove_file(path)
                    .expect("Something went wrong reading the file");

                Response::new(Body::from("success"))
            });

        let mut result: BoxFuture = Box::new(response);
        if forward {
            for peer in self.peers {
                let peer_request = peer.unlink(format!("{}", &self.filename));
                result = Box::new(result.join(peer_request).map(|(r, _)| r));
            }
        }

        return result;
    }

}

fn handler(req: Request<Body>, data_dir: String, peers: &[String]) -> BoxFuture {
    let mut response = Response::new(Body::empty());

    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap().trim_start_matches('/').to_string();

    let file = DistributedFile::new(filename, data_dir, peers);

    if req.method() == Method::POST && req.uri().path().starts_with("/truncate") {
        return file.truncate(req);
    }

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            return file.list_dir(req);
        },
        (&Method::GET, "/getattr") => {
            return file.getattr(req);
        },
        (&Method::DELETE, "/") => {
            return file.unlink(req);
        },
        (&Method::POST, _) => {
            return file.write(req);
        },
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        },
    };

    Box::new(future::ok(response))
}

fn handler_v2(request: ReadRequest, data_dir: String, peers: &[String]) -> ReadResponse {
    let file = DistributedFile::new(request.filename, data_dir, &peers);
    let data = file.read_v2(request.offset, request.size);
    ReadResponse {data}
}

struct ReadRequest {
    offset: u64,
    size: u32,
    filename: String
}

struct ReadResponse {
    data: Vec<u8>
}

struct ReadV2Codec {
}

impl Decoder for ReadV2Codec {
    type Item = ReadRequest;
    type Error = tokio::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ReadRequest>, tokio::io::Error> {
        if buf.len() < (8 + 4 + 2) {
            return Ok(None);
        }
        let offset = LittleEndian::read_u64(&buf[..8]);
        let size = LittleEndian::read_u32(&buf[8..12]);
        let path_size = LittleEndian::read_u16(&buf[12..14]);
        if buf.len() < (8 + 4 + 2 + path_size as usize) {
            return Ok(None);
        }
        let filename = String::from_utf8_lossy(&buf[14..(14 + path_size as usize)]).trim_start_matches('/').to_string();

        buf.split_to(8 + 4 + 2 + path_size as usize);

        Ok(Some(ReadRequest {
            offset,
            size,
            filename
        }))
    }
}

impl Encoder for ReadV2Codec {
    type Item = ReadResponse;
    type Error = tokio::io::Error;

    fn encode(&mut self, response: ReadResponse, buf: &mut BytesMut) -> Result<(), Self::Error> {
        buf.reserve(4 + response.data.len());
        buf.put_u32_le(response.data.len() as u32);
        buf.put(response.data);

        Ok(())
    }
}

pub struct Node {
    data_dir: String,
    port: u16,
    port_v2: u16,
    peers: Vec<String>
}

impl Node {
    pub fn new(data_dir: String, port: u16, port_v2: u16, peers: &[String]) -> Node {
        Node {
            data_dir,
            port,
            port_v2,
            peers: Vec::from(peers)
        }
    }

    pub fn run(self) {
        match fs::create_dir_all(&self.data_dir) {
            Err(why) => panic!("Couldn't create storage dir: {}", why.description()),
            Ok(_) => ()

        };

        let addr = ([127, 0, 0, 1], self.port).into();

        let data_dir = self.data_dir.clone();
        let peers = self.peers.clone();
        let new_service = move || {
            let data_dir2 = data_dir.clone();
            let peers2 = peers.clone();
            service_fn(move |req| handler(req, data_dir2.clone(), &peers2))
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        // TCP based v2 API
        let addr_v2 = ([127, 0, 0, 1], self.port_v2).into();
        let listener = TcpListener::bind(&addr_v2)
            .expect("unable to bind API v2 listener");

        let data_dir = self.data_dir.clone();
        let peers = self.peers.clone();
        let server_v2 = listener.incoming()
            .map_err(|e| eprintln!("accept connection failed = {:?}", e))
            .for_each(move |socket| {
                let framed_socket = Framed::new(socket, ReadV2Codec{});
                let (writer, reader) = framed_socket.split();
                let (mut tx, rx) = channel(0);
                let writer = writer.sink_map_err(|_| ());
                let sink = rx.forward(writer).map(|_| ());
                tokio::spawn(sink);

                let data_dir2 = data_dir.clone();
                let peers2 = peers.clone();

                let conn = reader.for_each(move |frame| {
                    let data_dir = data_dir2.clone();
                    let peers = peers2.clone();
                    let response = handler_v2(frame, data_dir, &peers);
                    let send = tx.start_send(response);
                    send.map(|_| ()).map_err(|_| tokio::io::Error::from(ErrorKind::Other))
                });

                tokio::spawn(conn.map_err(|_| ()))
            });

        hyper::rt::run(server.join(server_v2).map(|_| ()));
    }
}
