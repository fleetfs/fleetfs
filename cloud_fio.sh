#!/usr/bin/env bash

DIR=$(mktemp --directory)
until fleetfs --server-ip-port ${1} --get-leader; do
    sleep 0.1
done
fleetfs --server-ip-port ${1} --mount-point $DIR --direct-io -vvv &
sleep 3
fio --name read-test --eta-newline=5s --filename=${DIR}/fio-tempfile.dat --rw=read --size=10m --io_size=10g --blocksize=1m \
    --ioengine=libaio --fsync=1000 --iodepth=32 --direct=1 --numjobs=1 --runtime=20 --group_reporting > ${DIR}/${1}.results

sleep 100000
