use clap::Arg;
use clap::Command;
use clap::{ArgAction, crate_version};

use crate::client::NodeClient;
use crate::fuse_adapter::FleetFUSE;
use crate::storage::Node;
use log::LevelFilter;
use log::debug;
use log::warn;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use crate::base::ErrorCode;
use fuser::{Config, MountOption, SessionACL};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::thread::sleep;
use std::time::Duration;

pub mod base;
pub mod client;
pub mod fuse_adapter;
pub mod storage;

pub fn fuse_allow_other_enabled() -> io::Result<bool> {
    let file = File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        if line?.trim_start().starts_with("user_allow_other") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn main() -> Result<(), ErrorCode> {
    let matches = Command::new("FleetFS")
        .version(crate_version!())
        .author("Christopher Berner")
        .arg(
            Arg::new("port")
                .long("port")
                .value_name("PORT")
                .default_value("3000")
                .help("Set server port"),
        )
        .arg(
            Arg::new("bind-ip")
                .long("bind-ip")
                .value_name("BIND_IP")
                .default_value("127.0.0.1")
                .help("Address for server to listen on"),
        )
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .value_name("DIR")
                .default_value("/tmp/fleetfs")
                .help("Set local directory used to store data"),
        )
        .arg(
            Arg::new("peers")
                .long("peers")
                .value_name("PEERS")
                .default_value("")
                .help("Comma separated list of peer IP:PORT, or DNS-RECORD:PORT in which case DNS-RECORD must resolve to an A record containing --num-peers peers"),
        )
        .arg(
            Arg::new("num-peers")
                .long("num-peers")
                .value_name("NUM-PEERS")
                .default_value("0")
                .requires("peers")
                .help("Number of peer records to expect in the DNS record specified in --peers"),
        )
        .arg(
            Arg::new("redundancy-level")
                .long("redundancy-level")
                .value_name("REDUNDANCY-LEVEL")
                .requires("peers")
                .help("Number of failures that can be tolerated, in a replication group, before data is lost"),
        )
        .arg(
            Arg::new("server-ip-port")
                .long("server-ip-port")
                .value_name("IP_PORT")
                .default_value("127.0.0.1:3000")
                .help("Act as a client, and connect to given server"),
        )
        .arg(
            Arg::new("mount-point")
                .long("mount-point")
                .value_name("MOUNT_POINT")
                .default_value("")
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("direct-io")
                .long("direct-io")
                .action(ArgAction::SetTrue)
                .requires("mount-point")
                .help("Mount FUSE with direct IO"),
        )
        .arg(
            Arg::new("fsck")
                .long("fsck")
                .action(ArgAction::SetTrue)
                .help("Run a filesystem check on the cluster"),
        )
        .arg(
            Arg::new("get-leader")
                .long("get-leader")
                .action(ArgAction::SetTrue)
                .help("Print the ID of the leader node"),
        )
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    let verbosity = matches.get_count("v");
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

    let port: u16 = matches.get_one::<String>("port").unwrap().parse().unwrap();
    let data_dir: String = matches.get_one::<String>("data-dir").unwrap().to_string();
    let bind_ip: IpAddr = matches
        .get_one::<String>("bind-ip")
        .unwrap()
        .parse()
        .unwrap();
    let bind_address: SocketAddr = (bind_ip, port).into();
    let server_ip_port: SocketAddr = matches
        .get_one::<String>("server-ip-port")
        .unwrap()
        .parse()
        .unwrap();
    let mount_point: String = matches
        .get_one::<String>("mount-point")
        .unwrap()
        .to_string();
    let direct_io: bool = matches.get_flag("direct-io");
    let fsck: bool = matches.get_flag("fsck");
    let get_leader: bool = matches.get_flag("get-leader");
    let num_peers: usize = matches
        .get_one::<String>("num-peers")
        .unwrap()
        .parse()
        .unwrap();
    let peers: Vec<SocketAddr> = if num_peers > 0 {
        let record = format!("{}:{}", matches.get_one::<String>("peers").unwrap(), port);
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
            .get_one::<String>("peers")
            .unwrap()
            .split(',')
            .map(ToString::to_string)
            .filter(|x| !x.is_empty())
            .map(|x| x.parse().unwrap())
            .collect()
    };

    let replicas_per_raft_group: usize =
        if let Some(redundancy) = matches.get_one::<String>("redundancy-level") {
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
        let options = vec![
            MountOption::FSName("fleetfs".to_string()),
            MountOption::AutoUnmount,
        ];
        if direct_io {
            println!("Using Direct IO");
        }
        let allow_other = match fuse_allow_other_enabled() {
            Ok(enabled) => enabled,
            Err(_) => {
                eprintln!("Unable to read /etc/fuse.conf");
                false
            }
        };

        let fs = FleetFUSE::new(server_ip_port, direct_io);
        let mut config = Config::default();
        config.mount_options = options;
        config.acl = if allow_other {
            SessionACL::All
        } else {
            SessionACL::Owner
        };
        fuser::mount2(fs, &mount_point, &config).unwrap();
    }

    Ok(())
}
