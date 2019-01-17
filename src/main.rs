mod fleet;

extern crate bytes;
extern crate futures;
extern crate hyper;

use futures::future;
use futures::Stream;

use std::error::Error;

use std::io::Write;
use std::fs;

use bytes::Buf;

use hyper::{Body, Response, Server, StatusCode};
use hyper::rt::Future;
use hyper::Request;
use hyper::Method;
use hyper::service::service_fn;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::path::Path;
use std::fs::File;
use futures::future::ok;


type BoxFuture = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

fn write(req: Request<Body>) -> BoxFuture {
    let response = req.into_body()
        .concat2()
        .map(|chunk| {
        let bytes = chunk.bytes();
        let path = Path::new("junk/junk.txt");
        let display = path.display();

        let mut file = match File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}",
                               display,
                               why.description()),
            Ok(file) => file,
        };

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

fn read(req: Request<Body>) -> BoxFuture {
    let response = req.into_body()
        .concat2()
        .map(|chunk| {
            let contents = fs::read_to_string("junk/junk.txt")
                .expect("Something went wrong reading the file");

            Response::new(Body::from(contents))
        });

    Box::new(response)
}

fn handler(req: Request<Body>, state: &AtomicUsize) -> BoxFuture {
    let mut response = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            return read(req);
        },
        (&Method::POST, "/") => {
            return write(req);
        },
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        },
    };

    Box::new(future::ok(response))
}

fn main() {
    println!("Starting");
    let addr = ([127, 0, 0, 1], 3000).into();

    let counter = Arc::new(AtomicUsize::new(0));

    let new_service = move || {
        let counter2 = counter.clone();
        service_fn(move |req| handler(req, &counter2))
    };

    let server = Server::bind(&addr)
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));

    hyper::rt::run(server);
}

