#!/usr/bin/env bash

NC="\e[39m"
GREEN="\e[32m"
RED="\e[31m"

trap "exit" INT TERM
trap "kill 0" EXIT

DATA_DIR=$(mktemp --directory)
DIR=$(mktemp --directory)
cargo run -- --port 3300 --data-dir $DATA_DIR &
sleep 2
python client/main.py --server-url http://localhost:3300 --mount-point $DIR &
sleep 2
FUSE_PID=$!

echo "mounting at $DIR"

echo 1 > ${DIR}/1.txt
if [[ $(cat ${DIR}/1.txt) = "1" ]]; then
    echo -e "$GREEN OK 0 $NC"
else
    echo "$RED FAILED on 1.txt $NC"
fi

echo 2 > ${DIR}/1.txt
if [[ $(cat ${DIR}/1.txt) = "2" ]]; then
    echo -e "$GREEN OK 1 $NC"
else
    echo "$RED FAILED on rewrite 1.txt $NC"
fi

echo 2 > ${DIR}/2.txt
if [[ $(cat ${DIR}/2.txt) = "2" ]]; then
    echo -e "$GREEN OK 2 $NC"
else
    echo "$RED FAILED on 2.txt $NC"
fi

rm ${DIR}/2.txt
if [[ ! -f ${DIR}/2.txt ]]; then
    echo -e "$GREEN OK 3 $NC"
else
    echo "$RED FAILED deleting 2.txt $NC"
fi

yes 0123 | head -n 10000 > ${DIR}/big.txt
if [[ $(cat ${DIR}/big.txt | wc) = "$(yes 0123 | head -n 10000 | wc)" ]]; then
    echo -e "$GREEN OK 4 $NC"
else
    echo -e "$RED FAILED on big.txt $NC"
fi

kill $FUSE_PID
sleep 2

if rmdir ${DIR}; then
    echo -e "$GREEN OK 5 $NC"
else
    echo -e "$RED FAILED cleaning up mount point $NC"
fi

