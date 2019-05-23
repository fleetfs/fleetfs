#!/usr/bin/env bash

set -x

NC="\e[39m"
GREEN="\e[32m"
RED="\e[31m"

trap "exit" INT TERM
trap "kill 0" EXIT

BASELINE_DIR=$(mktemp --directory)
DATA_DIR=$(mktemp --directory)
DATA_DIR2=$(mktemp --directory)
DIR=$(mktemp --directory)
DIR2=$(mktemp --directory)
cargo build --release
cargo run --release -- --port 3300 --data-dir $DATA_DIR --peers 127.0.0.1:3301  &
cargo run --release -- --port 3301 --data-dir $DATA_DIR2 --peers 127.0.0.1:3300 &
sleep 0.5
cargo run -- --server-ip-port 127.0.0.1:3300 --get-leader
cargo run --release -- --server-ip-port 127.0.0.1:3300 --mount-point $DIR &
cargo run --release -- --server-ip-port 127.0.0.1:3301 --mount-point $DIR2 &
sleep 0.5

echo "mounting at $DIR"

echo -e "$GREEN FleetFS sequential read $NC"
fio --name read-test --eta-newline=5s --filename=${DIR}/fio-tempfile.dat --rw=read --size=500k --io_size=10g --blocksize=8k \
    --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting

echo -e "$GREEN FleetFS sequential read on second server $NC"
fio --name read-test --eta-newline=5s --filename=${DIR2}/fio-tempfile.dat --rw=read --size=500k --io_size=10g --blocksize=8k \
    --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting

rm ${DIR}/fio-tempfile.dat
