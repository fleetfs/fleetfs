#!/usr/bin/env bash

set -x

NC="\e[39m"
GREEN="\e[32m"
RED="\e[31m"

trap "exit" INT TERM
trap "kill 0" EXIT

DATA_DIR=$(mktemp --directory)
DATA_DIR2=$(mktemp --directory)
DIR=$(mktemp --directory)
DIR2=$(mktemp --directory)
cargo build
cargo run -- --port 3300 --port-v2 4300 --data-dir $DATA_DIR --peers http://localhost:3301 --peers-v2 127.0.0.1:4301 &
cargo run -- --port 3301 --port-v2 4301 --data-dir $DATA_DIR2 --peers http://localhost:3300 --peers-v2 127.0.0.1:4300 &
sleep 2
cargo run -- --server-url http://localhost:3300 --server-ip-port 127.0.0.1:4300 --mount-point $DIR &
FUSE_PID=$!
# Mount the replica with direct IO, so that replication shows up immediately. Otherwise, some tests might fail
# due to caching in the kernel
cargo run -- --server-url http://localhost:3301 --server-ip-port 127.0.0.1:4301 --mount-point $DIR2 --direct-io &
sleep 2

echo "mounting at $DIR"
echo "mounting replica at $DIR2"

echo 1 > ${DIR}/1.txt
if [[ $(cat ${DIR}/1.txt) = "1" ]]; then
    echo -e "$GREEN OK 0 $NC"
else
    echo -e "$RED FAILED on 1.txt $NC"
    exit
fi
if [[ $(cat ${DIR2}/1.txt) = "1" ]]; then
    echo -e "$GREEN OK 0 replica $NC"
else
    echo -e "$RED FAILED on 1.txt replica $NC"
    exit
fi

echo 2 > ${DIR}/1.txt
if [[ $(cat ${DIR}/1.txt) = "2" ]]; then
    echo -e "$GREEN OK 1 $NC"
else
    echo -e "$RED FAILED on rewrite 1.txt $NC"
    exit
fi
if [[ $(cat ${DIR2}/1.txt) = "2" ]]; then
    echo -e "$GREEN OK 1 replica $NC"
else
    echo -e "$RED FAILED on rewrite 1.txt replica $NC"
    exit
fi

echo 2 > ${DIR}/2.txt
if [[ $(cat ${DIR}/2.txt) = "2" ]]; then
    echo -e "$GREEN OK 2 $NC"
else
    echo -e "$RED FAILED on 2.txt $NC"
    exit
fi
if [[ $(cat ${DIR2}/2.txt) = "2" ]]; then
    echo -e "$GREEN OK 2 replica $NC"
else
    echo -e "$RED FAILED on 2.txt replica $NC"
    exit
fi

rm ${DIR}/2.txt
if [[ ! -f ${DIR}/2.txt ]]; then
    echo -e "$GREEN OK 3 $NC"
else
    echo -e "$RED FAILED deleting 2.txt $NC"
    exit
fi
if [[ ! -f ${DIR2}/2.txt ]]; then
    echo -e "$GREEN OK 3 replica $NC"
else
    echo -e "$RED FAILED deleting 2.txt replica $NC"
    exit
fi

yes 0123 | head -n 10000 > ${DIR}/big.txt
if [[ $(cat ${DIR}/big.txt | wc) = "$(yes 0123 | head -n 10000 | wc)" ]]; then
    echo -e "$GREEN OK 4 $NC"
else
    echo -e "$RED FAILED on big.txt $NC"
    exit
fi
if [[ $(cat ${DIR2}/big.txt | wc) = "$(yes 0123 | head -n 10000 | wc)" ]]; then
    echo -e "$GREEN OK 4 replica $NC"
else
    echo -e "$RED FAILED on big.txt replica $NC"
    exit
fi

echo 5 > ${DIR}/5.txt
mv ${DIR}/5.txt ${DIR}/new_5.txt
if [[ $(cat ${DIR}/new_5.txt) = "5" ]]; then
    echo -e "$GREEN OK 5 $NC"
else
    echo -e "$RED FAILED on mv 5.txt $NC"
    exit
fi
if [[ $(cat ${DIR2}/new_5.txt) = "5" ]]; then
    echo -e "$GREEN OK 5 replica $NC"
else
    echo -e "$RED FAILED on mv 5.txt replica $NC"
    exit
fi

touch -d "jan 3 2000" ${DIR}/new_5.txt
if [[ $(stat -c'%x' ${DIR}/new_5.txt) == 2000-01* ]]; then
    echo -e "$GREEN OK 6 $NC"
else
    echo -e "$RED FAILED on touch new_5.txt $NC"
    exit
fi
if [[ $(stat -c'%x' ${DIR2}/new_5.txt) == 2000-01* ]]; then
    echo -e "$GREEN OK 6 replica $NC"
else
    echo -e "$RED FAILED on touch new_5.txt replica $NC"
    exit
fi

chmod 747 ${DIR}/new_5.txt
if [[ $(stat -c'%a' ${DIR}/new_5.txt) == "747" ]]; then
    echo -e "$GREEN OK 7 $NC"
else
    echo -e "$RED FAILED on chmod new_5.txt $NC"
    exit
fi
if [[ $(stat -c'%a' ${DIR2}/new_5.txt) == "747" ]]; then
    echo -e "$GREEN OK 7 replica $NC"
else
    echo -e "$RED FAILED on chmod new_5.txt replica $NC"
    exit
fi

kill $FUSE_PID
sleep 2

if rmdir ${DIR}; then
    echo -e "$GREEN OK END $NC"
else
    echo -e "$RED FAILED cleaning up mount point $NC"
    exit
fi


