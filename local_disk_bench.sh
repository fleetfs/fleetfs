#!/usr/bin/env bash

set -x

NC="\e[39m"
GREEN="\e[32m"

BASELINE_DIR=$(mktemp --directory)

echo -e "$GREEN Local disk sequential read $NC"
fio --name read-test --eta-newline=5s --filename=${BASELINE_DIR}/fio-tempfile.dat --rw=read --size=500k --io_size=10g --blocksize=8k \
    --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting

rm -rf ${BASELINE_DIR}
