#!/usr/bin/env bash

DIR=$(mktemp --directory)
until fleetfs --server-ip-port ${1} --get-leader 2> /dev/null; do
    sleep 1
done
fleetfs --server-ip-port ${1} --mount-point $DIR --direct-io -vv &
sleep 3
fio --name read-test --eta-newline=5s --filename=${DIR}/fio-tempfile.dat --rw=read --size=10m --io_size=10g \
    --blocksize=1m --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting \
    --fallocate=none --output-format=json > ${DIR}/${1}.results

python /code/fleetfs/cloud_benchmark/aggregate_fio.py ${DIR} 2
sleep 100000
