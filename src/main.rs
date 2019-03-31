use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

use bytes::Buf;
use clap::crate_version;
use clap::App;
use clap::Arg;
use futures::future;
use futures::Stream;
use hyper::{Body, Response, Server, StatusCode};
use hyper::Method;
use hyper::Request;
use hyper::rt::Future;
use hyper::service::service_fn;

mod fleetfs;

const PATH_HEADER: &str = "X-FleetFS-Path";


type BoxFuture = Box<Future<Item=Response<Body>, Error=hyper::Error> + Send>;

fn truncate(req: Request<Body>, data_dir: String) -> BoxFuture {
    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();
    let response = req.into_body()
        .concat2()
        .map(move |_| {
            let path = Path::new(&data_dir).join(filename);
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

fn list_dir(req: Request<Body>, data_dir: String) -> BoxFuture {
    let dir_name: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();
    println!("Listing directory");
    let response = req.into_body()
        .concat2()
        .map(move |_| {
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
        .map(move |_| {
            let contents = fs::read_to_string(Path::new(&data_dir).join(filename))
                .expect("Something went wrong reading the file");

            Response::new(Body::from(contents))
        });

    Box::new(response)
}

fn unlink(req: Request<Body>, data_dir: String) -> BoxFuture {
    let filename: String = req.headers()[PATH_HEADER].to_str().unwrap()[1..].to_string();

    assert_ne!(filename.len(), 0);

    println!("Deleting file");
    let response = req.into_body()
        .concat2()
        .map(move |_| {
            fs::remove_file(Path::new(&data_dir).join(filename))
                .expect("Something went wrong reading the file");

            Response::new(Body::from("success"))
        });

    Box::new(response)
}

fn handler(req: Request<Body>, data_dir: String) -> BoxFuture {
    let mut response = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            return read(req, data_dir);
        },
        (&Method::DELETE, "/") => {
            return unlink(req, data_dir);
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
        .version(crate_version!())
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

    match fs::create_dir_all(&data_dir) {
        Err(why) => panic!("Couldn't create storage dir: {}", why.description()),
        Ok(_) => ()

    };

    println!("Starting");
    let addr = ([127, 0, 0, 1], port).into();

    let new_service = move || {
        let data_dir2 = data_dir.clone();
        service_fn(move |req| handler(req, data_dir2.clone()))
    };

    let server = Server::bind(&addr)
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));

    hyper::rt::run(server);
}

