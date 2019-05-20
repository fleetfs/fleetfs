#![allow(clippy::needless_return)]

use clap::crate_version;
use clap::App;
use clap::Arg;

use crate::client::NodeClient;
use crate::fuse::FleetFUSE;
use crate::storage_node::Node;
use log::LevelFilter;
use std::ffi::OsStr;
use std::net::SocketAddr;

use crate::generated::ErrorCode;

pub mod client;
pub mod distributed_file;
pub mod fuse;
pub mod local_storage;
pub mod peer_client;
pub mod raft_manager;
pub mod storage_node;
pub mod tcp_client;
pub mod utils;

include!(concat!(env!("OUT_DIR"), "/messages_generated.mod"));

fn main() -> Result<(), ErrorCode> {
    let matches = App::new("FleetFS")
        .version(crate_version!())
        .author("Christopher Berner")
        .arg(
            Arg::with_name("port")
                .long("port")
                .value_name("PORT")
                .default_value("3000")
                .help("Set server port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .value_name("DIR")
                .default_value("/tmp/fleetfs")
                .help("Set local directory used to store data")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("peers")
                .long("peers")
                .value_name("PEERS")
                .default_value("")
                .help("Comma separated list of peer IP:PORT")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("server-ip-port")
                .long("server-ip-port")
                .value_name("IP_PORT")
                .default_value("127.0.0.1:3000")
                .help("Act as a client, and connect to given server")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mount-point")
                .long("mount-point")
                .value_name("MOUNT_POINT")
                .default_value("")
                .help("Act as a client, and mount FUSE at given path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("direct-io")
                .long("direct-io")
                .requires("mount-point")
                .help("Mount FUSE with direct IO"),
        )
        .arg(
            Arg::with_name("fsck")
                .long("fsck")
                .help("Run a filesystem check on the cluster"),
        )
        .arg(
            Arg::with_name("get-leader")
                .long("get-leader")
                .help("Print the ID of the leader node"),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    let port: u16 = matches
        .value_of("port")
        .unwrap_or_default()
        .parse()
        .unwrap();
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    let server_ip_port: SocketAddr = matches
        .value_of("server-ip-port")
        .unwrap_or_default()
        .parse()
        .unwrap();
    let mount_point: String = matches
        .value_of("mount-point")
        .unwrap_or_default()
        .to_string();
    let direct_io: bool = matches.is_present("direct-io");
    let fsck: bool = matches.is_present("fsck");
    let get_leader: bool = matches.is_present("get-leader");
    let verbosity: u64 = matches.occurrences_of("v");
    let peers: Vec<SocketAddr> = matches
        .value_of("peers")
        .unwrap_or_default()
        .split(',')
        .map(ToString::to_string)
        .filter(|x| !x.is_empty())
        .map(|x| x.parse().unwrap())
        .collect();

    let log_level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    env_logger::builder()
        .default_format_timestamp_nanos(true)
        .filter_level(log_level)
        .init();

    if fsck {
        let client = NodeClient::new(server_ip_port);
        match client.fsck() {
            Ok(_) => println!("Filesystem is ok"),
            Err(e) => {
                match e {
                    ErrorCode::Corrupted => println!("Filesystem corrupted!"),
                    _ => println!("Filesystem check failed. Try again."),
                }
                return Err(e);
            }
        }
    } else if get_leader {
        let client = NodeClient::new(server_ip_port);
        println!("Leader: {}", client.leader_id()?);
    } else if mount_point.is_empty() {
        println!("Starting with peers: {:?}", &peers);
        Node::new(&data_dir, port, peers).run();
    } else {
        println!(
            "Connecting to server {} and mounting FUSE at {}",
            &server_ip_port, &mount_point
        );
        let mut fuse_args: Vec<&OsStr> = vec![&OsStr::new("-o")];
        if direct_io {
            println!("Using Direct IO");
            fuse_args.push(&OsStr::new("fsname=fleetfs,direct_io,auto_unmount"))
        } else {
            fuse_args.push(&OsStr::new("fsname=fleetfs,auto_unmount"))
        }
        let fs = FleetFUSE::new(server_ip_port);
        fuse_mt::mount(fuse_mt::FuseMT::new(fs, 1), &mount_point, &fuse_args).unwrap();
    }

    return Ok(());
}
