#!/usr/bin/env bash

set -ex

trap "exit" TERM
trap "kill 0" INT EXIT

export RUST_BACKTRACE=1

DATA_DIR=$(mktemp --directory)
DATA_DIR2=$(mktemp --directory)
DIR=$(mktemp --directory)
cargo build
cargo run -- --port 3300 --data-dir $DATA_DIR --peers 127.0.0.1:3301 > /code/logs/daemon0.log 2>&1 &
cargo run -- --port 3301 --data-dir $DATA_DIR2 --peers 127.0.0.1:3300 > /code/logs/daemon1.log 2>&1 &

# Wait for leader to be elected
sleep 0.5
cargo run -- --server-ip-port 127.0.0.1:3300 --get-leader

cargo run -- --server-ip-port 127.0.0.1:3300 --mount-point $DIR > /code/logs/mount.log 2>&1 &
FUSE_PID=$!
sleep 0.5

echo "mounting at $DIR"
# Make sure FUSE was successfully mounted
mount | grep fleetfs

set +e
cd ${DIR}
prove -rf /code/pjdfstest/tests | tee /code/logs/pjdfs.log

rm -rf ${DATA_DIR}
rm -rf ${DATA_DIR2}

kill $FUSE_PID
wait $FUSE_PID

