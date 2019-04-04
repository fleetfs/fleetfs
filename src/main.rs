use clap::crate_version;
use clap::App;
use clap::Arg;

use crate::fleetfs::core::Node;

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
        .get_matches();

    let port: u16 = matches.value_of("port").unwrap_or_default().parse().unwrap();
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    let peers: Vec<String> = matches.value_of("peers").unwrap_or_default()
        .split(",")
        .map(|x| x.to_string())
        .collect();

    println!("Starting");
    Node::new(data_dir, port, &peers).run();
}

