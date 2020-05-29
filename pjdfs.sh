#!/usr/bin/env bash

set -ex

exit_handler() {
    exit "$PJDFS_EXIT_STATUS"
}
trap exit_handler TERM
trap "kill 0" INT EXIT

export RUST_BACKTRACE=1

DATA_DIR=$(mktemp --directory)
DATA_DIR2=$(mktemp --directory)
DATA_DIR3=$(mktemp --directory)
DATA_DIR4=$(mktemp --directory)
DATA_DIR5=$(mktemp --directory)
DATA_DIR6=$(mktemp --directory)
DIR=$(mktemp --directory)
cargo build
cargo run -- --port 3300 --data-dir $DATA_DIR --peers 127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon0.log 2>&1 &
cargo run -- --port 3301 --data-dir $DATA_DIR2 --peers 127.0.0.1:3300,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon1.log 2>&1 &
cargo run -- --port 3302 --data-dir $DATA_DIR3 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3303,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon2.log 2>&1 &
cargo run -- --port 3303 --data-dir $DATA_DIR4 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon3.log 2>&1 &
cargo run -- --port 3304 --data-dir $DATA_DIR5 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3305 > /code/logs/daemon4.log 2>&1 &
cargo run -- --port 3305 --data-dir $DATA_DIR6 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3304 > /code/logs/daemon5.log 2>&1 &

# Wait for leader to be elected
until cargo run --release -- --server-ip-port 127.0.0.1:3300 --get-leader; do
    sleep 0.1
done

cargo run -- --server-ip-port 127.0.0.1:3300 --mount-point $DIR > /code/logs/mount.log 2>&1 &
FUSE_PID=$!
sleep 0.5

echo "mounting at $DIR"
# Make sure FUSE was successfully mounted
mount | grep fleetfs

set +e
cd ${DIR}
prove -rf /code/pjdfstest/tests | tee /code/logs/pjdfs.log
export PJDFS_EXIT_STATUS=${PIPESTATUS[0]}
echo "Total failed:"
cat /code/logs/pjdfs.log | egrep -o 'Failed: [0-9]+' | egrep -o '[0-9]+' | paste -s -d+ | bc

rm -rf ${DATA_DIR}
rm -rf ${DATA_DIR2}
rm -rf ${DATA_DIR3}
rm -rf ${DATA_DIR4}
rm -rf ${DATA_DIR5}
rm -rf ${DATA_DIR6}

kill $FUSE_PID
wait $FUSE_PID
