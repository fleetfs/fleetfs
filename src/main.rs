mod fleetfs;

extern crate bytes;
extern crate futures;
extern crate hyper;
extern crate clap;

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
use std::io::SeekFrom;
use std::io::Seek;
use std::fs::OpenOptions;
use clap::App;
use clap::Arg;
use clap::ArgMatches;

const PATH_HEADER: &str = "X-FleetFS-Path";


type BoxFuture = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

fn truncate(req: Request<Body>, data_dir: String) -> BoxFuture {
    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();
    let response = req.into_body()
        .concat2()
        .map(move |chunk| {
            let bytes = chunk.bytes();
            let path = Path::new(&data_dir).join(filename);
            let display = path.display();

            let mut file = match File::create(&path) {
                Err(why) => panic!("couldn't create {}: {}",
                                   display,
                                   why.description()),
                Ok(file) => file,
            };

            Response::new(Body::from("done"))
        });

    Box::new(response)
}

fn write(req: Request<Body>, data_dir: String) -> BoxFuture {
    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();
    let offset: u64 = req.uri().path()[1..].parse().unwrap();
    let response = req.into_body()
        .concat2()
        .map(move |chunk| {
        let bytes = chunk.bytes();
        let path = Path::new(&data_dir).join(filename);
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

        file.seek(SeekFrom::Start(offset));

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

fn list_dir(req: Request<Body>, data_dir: String) -> BoxFuture {
    let dir_name: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();
    println!("Listing directory");
    let response = req.into_body()
        .concat2()
        .map(move |chunk| {
            let mut entries = vec![];
            for entry in fs::read_dir(Path::new(&data_dir).join(dir_name)).unwrap() {
                let filename = entry.unwrap().path().clone().to_str().unwrap().to_string();
                // TODO: there must be a better way to strip the data_dir substring off the left side
                entries.push(filename.split_at(data_dir.len() + 1).1.to_string());
            }

            Response::new(Body::from(serde_json::to_string(&entries).unwrap()))
        });

    Box::new(response)
}

fn read(req: Request<Body>, data_dir: String) -> BoxFuture {
    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();

    if filename.len() == 0 {
        return list_dir(req, data_dir);
    }

    println!("Reading file");
    let response = req.into_body()
        .concat2()
        .map(move |chunk| {
            let contents = fs::read_to_string(Path::new(&data_dir).join(filename))
                .expect("Something went wrong reading the file");

            Response::new(Body::from(contents))
        });

    Box::new(response)
}

fn handler(req: Request<Body>, state: &AtomicUsize, data_dir: String) -> BoxFuture {
    let mut response = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            return read(req, data_dir);
        },
        (&Method::POST, "/truncate") => {
            return truncate(req, data_dir);
        },
        (&Method::POST, _) => {
            return write(req, data_dir);
        },
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        },
    };

    Box::new(future::ok(response))
}

fn main() {
    let matches = App::new("FleetFS")
        .version("0.0.0")
        .author("Christopher Berner")
        .arg(Arg::with_name("port")
            .long("port")
            .value_name("PORT")
            .default_value("3000")
            .help("Set server port")
            .takes_value(true))
        .arg(Arg::with_name("data-dir")
            .long("data-dir")
            .value_name("DIR")
            .default_value("/tmp/fleetfs")
            .help("Set local directory used to store data")
            .takes_value(true))
        .get_matches();

    let port: u16 = matches.value_of("port").unwrap_or_default().parse().unwrap();
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();

    let path = Path::new(&data_dir).join("junk.txt");
    let display = path.display();

    match fs::create_dir_all(&data_dir) {
        Err(why) => panic!("Couldn't create storage dir: {}", why.description()),
        Ok(_) => ()

    };
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path) {
        Err(why) => panic!("couldn't create {}: {}",
                           display,
                           why.description()),
        Ok(file) => file,
    };

    println!("Starting");
    let addr = ([127, 0, 0, 1], port).into();

    let counter = Arc::new(AtomicUsize::new(0));

    let new_service = move || {
        let counter2 = counter.clone();
        let data_dir2 = data_dir.clone();
        service_fn(move |req| handler(req, &counter2, data_dir2.clone()))
    };

    let server = Server::bind(&addr)
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));

    hyper::rt::run(server);
}

