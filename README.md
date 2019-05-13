# fleetfs
[![Build Status](https://travis-ci.com/fleetfs/fleetfs.svg?branch=master)](https://travis-ci.com/fleetfs/fleetfs)
[![Crates](https://img.shields.io/crates/v/fleetfs.svg)](https://crates.io/crates/fleetfs)
[![Documentation](https://docs.rs/fleetfs/badge.svg)](https://docs.rs/fleetfs)
[![dependency status](https://deps.rs/repo/github/fleetfs/fleetfs/status.svg)](https://deps.rs/repo/github/fleetfs/fleetfs)

FleetFS distributed filesystem

## Development
* `apt install libfuse-dev` (needed by `fuse` dependency)
* Install flatc (https://github.com/google/flatbuffers)
* rustup component add rustfmt

## Status
Very very alpha. Expect FleetFS to eat your data :)

**Features implemented:**
* basic operations: read/write/create/delete/rename
* file permissions (read/write/exec), but not ownership

