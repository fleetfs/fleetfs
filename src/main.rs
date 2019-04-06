use clap::crate_version;
use clap::App;
use clap::Arg;

use crate::fleetfs::core::Node;
use log::LevelFilter;

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
        .arg(Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity"))
        .get_matches();

    let port: u16 = matches.value_of("port").unwrap_or_default().parse().unwrap();
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    let verbosity: u64 = matches.occurrences_of("v");
    let peers: Vec<String> = matches.value_of("peers").unwrap_or_default()
        .split(",")
        .map(|x| x.to_string())
        .filter(|x| x.len() > 0)
        .collect();

    let log_level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace
    };

    env_logger::builder().filter_level(log_level).init();

    println!("Starting with peers: {:?}", &peers);
    Node::new(data_dir, port, &peers).run();
}

