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
cargo run -- --port 3300 --data-dir $DATA_DIR --peers http://localhost:3301 &
cargo run -- --port 3301 --data-dir $DATA_DIR2 --peers http://localhost:3300 &
sleep 2
python client/main.py --server-url http://localhost:3300 --mount-point $DIR &
sleep 2

echo "mounting at $DIR"

echo -e "$GREEN Local disk sequential read $NC"
fio --name read-test --eta-newline=5s --filename=${BASELINE_DIR}/fio-tempfile.dat --rw=read --size=500k --io_size=10g --blocksize=8k \
    --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting

rm -rf ${BASELINE_DIR}

echo -e "$GREEN FleetFS sequential read $NC"
fio --name read-test --eta-newline=5s --filename=${DIR}/fio-tempfile.dat --rw=read --size=500k --io_size=10g --blocksize=8k \
    --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting

rm ${DIR}/fio-tempfile.dat
