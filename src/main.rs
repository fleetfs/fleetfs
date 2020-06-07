#![allow(clippy::needless_return)]

use clap::crate_version;
use clap::App;
use clap::Arg;

use crate::client::NodeClient;
use crate::fuse_adapter::FleetFUSE;
use crate::storage::Node;
use log::debug;
use log::warn;
use log::LevelFilter;
use std::ffi::OsStr;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use crate::base::utils::fuse_allow_other_enabled;
use crate::generated::ErrorCode;
use std::thread::sleep;
use std::time::Duration;

pub mod base;
pub mod client;
pub mod fuse_adapter;
pub mod storage;

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
            Arg::with_name("bind-ip")
                .long("bind-ip")
                .value_name("BIND_IP")
                .default_value("127.0.0.1")
                .help("Address for server to listen on")
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
                .help("Comma separated list of peer IP:PORT, or DNS-RECORD:PORT in which case DNS-RECORD must resolve to an A record containing --num-peers peers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("num-peers")
                .long("num-peers")
                .value_name("NUM-PEERS")
                .default_value("0")
                .requires("peers")
                .help("Number of peer records to expect in the DNS record specified in --peers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("redundancy-level")
                .long("redundancy-level")
                .value_name("REDUNDANCY-LEVEL")
                .requires("peers")
                .help("Number of failures that can be tolerated, in a replication group, before data is lost")
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

    let verbosity: u64 = matches.occurrences_of("v");
    let log_level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(log_level)
        .init();

    let port: u16 = matches
        .value_of("port")
        .unwrap_or_default()
        .parse()
        .unwrap();
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    let bind_ip: IpAddr = matches
        .value_of("bind-ip")
        .unwrap_or_default()
        .parse()
        .unwrap();
    let bind_address: SocketAddr = (bind_ip, port).into();
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
    let num_peers: usize = matches
        .value_of("num-peers")
        .unwrap_or_default()
        .parse()
        .unwrap();
    let peers: Vec<SocketAddr> = if num_peers > 0 {
        let record = format!("{}:{}", matches.value_of("peers").unwrap_or_default(), port);
        let mut found_peers: Vec<SocketAddr> = match record.to_socket_addrs() {
            Ok(addresses) => addresses.collect(),
            Err(error) => {
                warn!("Encountered error in DNS lookup of {}: {:?}", record, error);
                vec![]
            }
        };
        while found_peers.len() < num_peers {
            found_peers = match record.to_socket_addrs() {
                Ok(addresses) => addresses.collect(),
                Err(error) => {
                    warn!("Encountered error in DNS lookup of {}: {:?}", record, error);
                    vec![]
                }
            };
            debug!(
                "Found {:?} peers. Waiting for {} peers",
                found_peers, num_peers
            );
            sleep(Duration::from_secs(1));
        }

        found_peers.retain(|x| *x != bind_address);
        found_peers
    } else {
        matches
            .value_of("peers")
            .unwrap_or_default()
            .split(',')
            .map(ToString::to_string)
            .filter(|x| !x.is_empty())
            .map(|x| x.parse().unwrap())
            .collect()
    };

    let replicas_per_raft_group: usize =
        if let Some(redundancy) = matches.value_of("redundancy-level") {
            let redundancy: usize = redundancy.parse().unwrap();
            2 * redundancy + 1
        } else {
            peers.len() + 1
        };

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
        client.filesystem_ready()?;
        println!("Filesystem ready");
    } else if mount_point.is_empty() {
        println!("Starting with peers: {:?}", &peers);
        Node::new(&data_dir, bind_address, peers, replicas_per_raft_group).run();
    } else {
        println!(
            "Connecting to server {} and mounting FUSE at {}",
            &server_ip_port, &mount_point
        );
        let mut fuse_args: Vec<&OsStr> = vec![&OsStr::new("-o")];
        let mut options = "fsname=fleetfs,auto_unmount".to_string();
        if direct_io {
            println!("Using Direct IO");
            options.push_str(",direct_io");
        }
        if let Ok(enabled) = fuse_allow_other_enabled() {
            if enabled {
                options.push_str(",allow_other");
            }
        } else {
            eprintln!("Unable to read /etc/fuse.conf");
        }

        fuse_args.push(&OsStr::new(&options));
        let fs = FleetFUSE::new(server_ip_port);
        fuse::mount(fs, &mount_point, &fuse_args).unwrap();
    }

    return Ok(());
}
