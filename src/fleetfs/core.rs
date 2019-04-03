use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

use bytes::Buf;
use futures::future;
use futures::Stream;
use hyper::{Body, Response, Server, StatusCode};
use hyper::Method;
use hyper::Request;
use hyper::rt::Future;
use hyper::service::service_fn;

const PATH_HEADER: &str = "X-FleetFS-Path";


type BoxFuture = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

struct DistributedFile {
    filename: String,
    local_data_dir: String
}

impl DistributedFile {
    fn new(filename: String, local_data_dir: String) -> DistributedFile {
        DistributedFile {
            filename,
            local_data_dir
        }
    }

    fn truncate(self, req: Request<Body>) -> BoxFuture {
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                let path = Path::new(&self.local_data_dir).join(self.filename);
                let display = path.display();

                match File::create(&path) {
                    Err(why) => panic!("couldn't create {}: {}",
                                       display,
                                       why.description()),
                    Ok(file) => file,
                };

                Response::new(Body::from("done"))
            });

        Box::new(response)
    }

    fn write(self, req: Request<Body>) -> BoxFuture {
        let offset: u64 = req.uri().path()[1..].parse().unwrap();
        let response = req.into_body()
            .concat2()
            .map(move |chunk| {
                let bytes = chunk.bytes();
                let path = Path::new(&self.local_data_dir).join(self.filename);
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
                    Ok(_) => println!("successfully wrote to {}", display),
                }

                Response::new(Body::from("done"))
            });

        Box::new(response)
    }

    fn list_dir(self, req: Request<Body>) -> BoxFuture {
        println!("Listing directory");
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

    fn read(self, req: Request<Body>) -> BoxFuture {
        if self.filename.len() == 0 {
            return self.list_dir(req);
        }

        println!("Reading file");
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                let contents = fs::read_to_string(Path::new(&self.local_data_dir).join(self.filename))
                    .expect("Something went wrong reading the file");

                Response::new(Body::from(contents))
            });

        Box::new(response)
    }

    fn unlink(self, req: Request<Body>) -> BoxFuture {
        assert_ne!(self.filename.len(), 0);

        println!("Deleting file");
        let response = req.into_body()
            .concat2()
            .map(move |_| {
                fs::remove_file(Path::new(&self.local_data_dir).join(self.filename))
                    .expect("Something went wrong reading the file");

                Response::new(Body::from("success"))
            });

        Box::new(response)
    }

}

fn handler(req: Request<Body>, data_dir: String) -> BoxFuture {
    let mut response = Response::new(Body::empty());

    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();

    let file = DistributedFile::new(filename, data_dir);

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            return file.read(req);
        },
        (&Method::DELETE, "/") => {
            return file.unlink(req);
        },
        (&Method::POST, "/truncate") => {
            return file.truncate(req);
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

pub struct Node {
    data_dir: String,
    port: u16
}

impl Node {
    pub fn new(data_dir: String, port: u16) -> Node {
        Node {
            data_dir,
            port
        }
    }

    pub fn run(self) {
        match fs::create_dir_all(&self.data_dir) {
            Err(why) => panic!("Couldn't create storage dir: {}", why.description()),
            Ok(_) => ()

        };

        let addr = ([127, 0, 0, 1], self.port).into();

        let new_service = move || {
            let data_dir2 = self.data_dir.clone();
            service_fn(move |req| handler(req, data_dir2.clone()))
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        hyper::rt::run(server);
    }
}
