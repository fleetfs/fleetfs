# FleetFS
[![Build Status](https://travis-ci.com/fleetfs/fleetfs.svg?branch=master)](https://travis-ci.com/fleetfs/fleetfs)
[![Crates](https://img.shields.io/crates/v/fleetfs.svg)](https://crates.io/crates/fleetfs)
[![Documentation](https://docs.rs/fleetfs/badge.svg)](https://docs.rs/fleetfs)
[![dependency status](https://deps.rs/repo/github/fleetfs/fleetfs/status.svg)](https://deps.rs/repo/github/fleetfs/fleetfs)

FleetFS distributed filesystem

## Development
* `apt install libfuse-dev` (needed by `fuse` dependency)
* Install flatc (https://github.com/google/flatbuffers)
* rustup component add rustfmt
* rustup component add clippy

## Status
Very very alpha. Expect FleetFS to eat your data :)

## Design decisions
* Clients only need to talk to a single node
  * Context: There is significant overhead in opening TCP connections, so we want the client to keep its
  connections open. Therefore, the client shouldn't make on-demand connections to every storage node in
  the cluster.
  * Cons: doubles network traffic inside FleetFS cluster, as nodes have to proxy traffic reading/writing
  to other nodes
  * Pros: better scalability, as client connection is handled by a single node. Also simplifies client code
* Clients are trusted to make permission checks
  * Context: FleetFS has no access to a central user store, so has to trust the user ids sent by the client
  * Cons: security relies on the client
  * Pros: client doesn't have to send exhaustive list of groups that user is part of to make permission checks
* Sharding design:
  * "Raft group" (or rgroup): a subset of storage nodes participating in a raft consensus group.
  The cluster consists of multiple Raft groups, and a single node may be part of multiple groups.
  * An inode is stored on a single Raft group, and inodes are sharded among groups, by id.
  * (TODO) "Redundant block" (or rblock) is a block of data stored in a single Raft group.
  A file's contents is composed of multiple rblocks, on one or more Raft groups.

## License

Licensed under

 * Apache License, Version 2.0 ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be licensed as above, without any
additional terms or conditions.
