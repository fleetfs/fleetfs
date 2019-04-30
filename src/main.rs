use clap::crate_version;
use clap::App;
use clap::Arg;

use crate::fleetfs::core::Node;
use log::LevelFilter;
use crate::fleetfs::fuse::FleetFUSE;
use std::ffi::OsStr;
use std::net::SocketAddr;

mod fleetfs;

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
        .arg(Arg::with_name("port-v2")
            .long("port-v2")
            .value_name("PORT")
            .default_value("4000")
            .help("Set server port for v2 API")
            .takes_value(true))
        .arg(Arg::with_name("data-dir")
            .long("data-dir")
            .value_name("DIR")
            .default_value("/tmp/fleetfs")
            .help("Set local directory used to store data")
            .takes_value(true))
        .arg(Arg::with_name("peers")
            .long("peers")
            .value_name("PEERS")
            .default_value("")
            .help("Comma separated list of peer URLs")
            .takes_value(true))
        .arg(Arg::with_name("peers-v2")
            .long("peers-v2")
            .value_name("PEERS-V2")
            .default_value("")
            .help("Comma separated list of peer IP:PORT")
            .takes_value(true))
        .arg(Arg::with_name("server-url")
            .long("server-url")
            .value_name("SERVER_URL")
            .default_value("http://localhost:3000")
            .help("Act as a client, and connect to given URL")
            .takes_value(true))
        .arg(Arg::with_name("server-v2-ip-port")
            .long("server-ip-port")
            .value_name("IP_PORT")
            .default_value("127.0.0.1:4000")
            .help("Act as a client, and connect to given server")
            .takes_value(true))
        .arg(Arg::with_name("mount-point")
            .long("mount-point")
            .value_name("SERVER_URL")
            .default_value("")
            .help("Act as a client, and mount FUSE at given path")
            .takes_value(true))
        .arg(Arg::with_name("direct-io")
            .long("direct-io")
            .requires("mount-point")
            .help("Mount FUSE with direct IO"))
        .arg(Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity"))
        .get_matches();

    let port: u16 = matches.value_of("port").unwrap_or_default().parse().unwrap();
    let port_v2: u16 = matches.value_of("port-v2").unwrap_or_default().parse().unwrap();
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    let server_url: String = matches.value_of("server-url").unwrap_or_default().to_string();
    let server_ip_port: SocketAddr = matches.value_of("server-v2-ip-port").unwrap_or_default().parse().unwrap();
    let mount_point: String = matches.value_of("mount-point").unwrap_or_default().to_string();
    let direct_io: bool = matches.is_present("direct-io");
    let verbosity: u64 = matches.occurrences_of("v");
    let peers: Vec<String> = matches.value_of("peers").unwrap_or_default()
        .split(",")
        .map(|x| x.to_string())
        .filter(|x| x.len() > 0)
        .collect();
    let peers_v2: Vec<SocketAddr> = matches.value_of("peers-v2").unwrap_or_default()
        .split(",")
        .map(|x| x.to_string())
        .filter(|x| x.len() > 0)
        .map(|x| x.parse().unwrap())
        .collect();

    let log_level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace
    };

    env_logger::builder().default_format_timestamp_nanos(true).filter_level(log_level).init();

    if mount_point.is_empty() {
        println!("Starting with peers: {:?}", &peers);
        Node::new(data_dir, port, port_v2, peers, peers_v2).run();
    }
    else {
        println!("Connecting to server {} and mounting FUSE at {}", &server_url, &mount_point);
        let mut fuse_args: Vec<&OsStr> = vec![&OsStr::new("-o")];
        if direct_io {
            println!("Using Direct IO");
            fuse_args.push(&OsStr::new("fsname=fleetfs,direct_io,auto_unmount"))
        }
        else {
            fuse_args.push(&OsStr::new("fsname=fleetfs,auto_unmount"))
        }
        let fs = FleetFUSE::new(server_url, server_ip_port);
        fuse_mt::mount(fuse_mt::FuseMT::new(fs, 1), &mount_point, &fuse_args).unwrap();
    }
}

