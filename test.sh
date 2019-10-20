#!/usr/bin/env bash

set -x

NC="\e[39m"
GREEN="\e[32m"
RED="\e[31m"

exit_handler() {
    exit "$TEST_EXIT_STATUS"
}
trap exit_handler TERM
trap 'kill $(jobs -p)' INT EXIT

export RUST_BACKTRACE=1

DATA_DIR=$(mktemp --directory)
DATA_DIR2=$(mktemp --directory)
DIR=$(mktemp --directory)
DIR2=$(mktemp --directory)
cargo build
cargo run -- --port 3300 --data-dir $DATA_DIR --peers 127.0.0.1:3301 &
cargo run -- --port 3301 --data-dir $DATA_DIR2 --peers 127.0.0.1:3300 &

# Wait for leader to be elected
until cargo run -- --server-ip-port 127.0.0.1:3300 --get-leader; do
    sleep 0.1
done

cargo run -- --server-ip-port 127.0.0.1:3300 --mount-point $DIR &
FUSE_PID=$!
# Mount the replica with direct IO, so that replication shows up immediately. Otherwise, some tests might fail
# due to caching in the kernel
cargo run -- --server-ip-port 127.0.0.1:3301 --mount-point $DIR2 --direct-io &
sleep 0.5

echo "mounting at $DIR"
echo "mounting replica at $DIR2"

echo 1 > ${DIR}/1.txt
if [[ $(cat ${DIR}/1.txt) = "1" ]]; then
    echo -e "$GREEN OK 0 $NC"
else
    echo -e "$RED FAILED on 1.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(cat ${DIR2}/1.txt) = "1" ]]; then
    echo -e "$GREEN OK 0 replica $NC"
else
    echo -e "$RED FAILED on 1.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

# TODO: support an IOCTL, and test that it works
# This is just to test that it doesn't *crash* the FUSE mount
if ! python3 -c "import fcntl; import termios; a = open('${DIR}/1.txt'); fcntl.ioctl(a, termios.TIOCGPGRP, '')"; then
    echo -e "$GREEN OK 0.ioctl $NC"
else
    echo -e "$RED FAILED on 1.txt ioctl $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

echo 2 > ${DIR}/1.txt
if [[ $(cat ${DIR}/1.txt) = "2" ]]; then
    echo -e "$GREEN OK 1 $NC"
else
    echo -e "$RED FAILED on rewrite 1.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(cat ${DIR2}/1.txt) = "2" ]]; then
    echo -e "$GREEN OK 1 replica $NC"
else
    echo -e "$RED FAILED on rewrite 1.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

echo 2 > ${DIR}/2.txt
if [[ $(cat ${DIR}/2.txt) = "2" ]]; then
    echo -e "$GREEN OK 2 $NC"
else
    echo -e "$RED FAILED on 2.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(cat ${DIR2}/2.txt) = "2" ]]; then
    echo -e "$GREEN OK 2 replica $NC"
else
    echo -e "$RED FAILED on 2.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

rm ${DIR}/2.txt
if [[ ! -f ${DIR}/2.txt ]]; then
    echo -e "$GREEN OK 3 $NC"
else
    echo -e "$RED FAILED deleting 2.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ ! -f ${DIR2}/2.txt ]]; then
    echo -e "$GREEN OK 3 replica $NC"
else
    echo -e "$RED FAILED deleting 2.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

yes 0123 | head -n 10000 > ${DIR}/big.txt
if [[ $(cat ${DIR}/big.txt | wc) = "$(yes 0123 | head -n 10000 | wc)" ]]; then
    echo -e "$GREEN OK 4 $NC"
else
    echo -e "$RED FAILED on big.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(cat ${DIR2}/big.txt | wc) = "$(yes 0123 | head -n 10000 | wc)" ]]; then
    echo -e "$GREEN OK 4 replica $NC"
else
    echo -e "$RED FAILED on big.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

echo 5 > ${DIR}/5.txt
mv ${DIR}/5.txt ${DIR}/new_5.txt
if [[ $(cat ${DIR}/new_5.txt) = "5" ]]; then
    echo -e "$GREEN OK 5 $NC"
else
    echo -e "$RED FAILED on mv 5.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(cat ${DIR2}/new_5.txt) = "5" ]]; then
    echo -e "$GREEN OK 5 replica $NC"
else
    echo -e "$RED FAILED on mv 5.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

touch -d "jan 3 2000" ${DIR}/new_5.txt
if [[ $(stat -c'%x' ${DIR}/new_5.txt) == 2000-01* ]]; then
    echo -e "$GREEN OK 6 $NC"
else
    echo -e "$RED FAILED on touch new_5.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(stat -c'%x' ${DIR2}/new_5.txt) == 2000-01* ]]; then
    echo -e "$GREEN OK 6 replica $NC"
else
    echo -e "$RED FAILED on touch new_5.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

chmod 747 ${DIR}/new_5.txt
if [[ $(stat -c'%a' ${DIR}/new_5.txt) == "747" ]]; then
    echo -e "$GREEN OK 7 $NC"
else
    echo -e "$RED FAILED on chmod new_5.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(stat -c'%a' ${DIR2}/new_5.txt) == "747" ]]; then
    echo -e "$GREEN OK 7 replica $NC"
else
    echo -e "$RED FAILED on chmod new_5.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

link ${DIR}/new_5.txt ${DIR}/hardlinked_5.txt
if [[ $(stat -c'%h' ${DIR}/new_5.txt) == "2" ]] && [[ $(stat -c'%h' ${DIR}/hardlinked_5.txt) == "2" ]]; then
    echo -e "$GREEN OK 8 $NC"
else
    echo -e "$RED FAILED on hardlink new_5.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(stat -c'%h' ${DIR2}/new_5.txt) == "2" ]] && [[ $(stat -c'%h' ${DIR2}/hardlinked_5.txt) == "2" ]]; then
    echo -e "$GREEN OK 8 replica $NC"
else
    echo -e "$RED FAILED on hardlink new_5.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

mkdir ${DIR}/sub
echo s > ${DIR}/sub/s.txt
if [[ $(cat ${DIR}/sub/s.txt) = "s" ]]; then
    echo -e "$GREEN OK 9 $NC"
else
    echo -e "$RED FAILED on s.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(cat ${DIR2}/sub/s.txt) = "s" ]]; then
    echo -e "$GREEN OK 9 replica $NC"
else
    echo -e "$RED FAILED on s.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

if cargo run -- --server-ip-port 127.0.0.1:3300 --fsck > /dev/null 2>&1; then
    echo -e "$GREEN OK 10 $NC"
else
    echo -e "$RED FAILED on fsck $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if cargo run -- --server-ip-port 127.0.0.1:3301 --fsck > /dev/null 2>&1; then
    echo -e "$GREEN OK 10 replica $NC"
else
    echo -e "$RED FAILED on fsck replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

rm ${DATA_DIR2}/data/2
if ! cargo run -- --server-ip-port 127.0.0.1:3300 --fsck > /dev/null 2>&1; then
    echo -e "$GREEN OK 11 $NC"
else
    echo -e "$RED FAILED on fsck corruption detection $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

touch ${DIR}/xattr.txt
xattr -w user.test hello_world ${DIR}/xattr.txt
if [[ $(xattr -p user.test ${DIR}/xattr.txt) = "hello_world" ]]; then
    echo -e "$GREEN OK 12 $NC"
else
    echo -e "$RED FAILED on xattr.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(xattr -p user.test ${DIR2}/xattr.txt) = "hello_world" ]]; then
    echo -e "$GREEN OK 12 replica $NC"
else
    echo -e "$RED FAILED on xattr.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

if [[ $(xattr ${DIR}/xattr.txt) = "user.test" ]]; then
    echo -e "$GREEN OK 13 $NC"
else
    echo -e "$RED FAILED on xattr.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(xattr ${DIR2}/xattr.txt) = "user.test" ]]; then
    echo -e "$GREEN OK 13 replica $NC"
else
    echo -e "$RED FAILED on xattr.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

xattr -d user.test ${DIR}/xattr.txt
if [[ $(xattr ${DIR}/xattr.txt) = "" ]]; then
    echo -e "$GREEN OK 14 $NC"
else
    echo -e "$RED FAILED on remove xattr.txt $NC"
    export TEST_EXIT_STATUS=1
    exit
fi
if [[ $(xattr ${DIR2}/xattr.txt) = "" ]]; then
    echo -e "$GREEN OK 14 replica $NC"
else
    echo -e "$RED FAILED on remove xattr.txt replica $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

kill $FUSE_PID
wait $FUSE_PID

if rmdir ${DIR}; then
    echo -e "$GREEN OK END $NC"
else
    echo -e "$RED FAILED cleaning up mount point $NC"
    export TEST_EXIT_STATUS=1
    exit
fi

export TEST_EXIT_STATUS=0
