#!/usr/bin/env bash

set -ex

exit_handler() {
    exit "$XFSTESTS_EXIT_STATUS"
}
trap exit_handler TERM
trap "kill 0" INT EXIT

export RUST_BACKTRACE=1

DATA_DIR=$(mktemp --directory)
DATA_DIR2=$(mktemp --directory)
cargo build --release
# Copy into PATH, so that xfstests can find the binary
cp target/release/fleetfs /bin/fleetfs

cargo run --release -- --port 3300 --data-dir $DATA_DIR --peers 127.0.0.1:3301 > /code/logs/daemon0.log 2>&1 &
cargo run --release -- --port 3301 --data-dir $DATA_DIR2 --peers 127.0.0.1:3300 > /code/logs/daemon1.log 2>&1 &

SCRATCH_DIR=$(mktemp --directory)
SCRATCH_DIR2=$(mktemp --directory)
cargo run --release -- --port 3400 --data-dir $SCRATCH_DIR --peers 127.0.0.1:3401 > /code/logs/scratch0.log 2>&1 &
cargo run --release -- --port 3401 --data-dir $SCRATCH_DIR2 --peers 127.0.0.1:3400 > /code/logs/scratch1.log 2>&1 &

# Wait for leaders to be elected
sleep 0.5
cargo run --release -- --server-ip-port 127.0.0.1:3300 --get-leader
cargo run --release -- --server-ip-port 127.0.0.1:3400 --get-leader

sleep 0.5

set +e
# Clear mount log file, since the tests append to it
echo "" > /code/logs/xfstests_mount.log
DIR=/var/tmp/fuse-xfstests/check-fleetfs
mkdir -p $DIR
cd /code/fuse-xfstests

# TODO: Fix test 113. It hangs
echo "generic/113" > xfs_excludes.txt
# TODO: Fix test 258. Crashes server due to overflow in rust-fuse
echo "generic/258" >> xfs_excludes.txt
# TODO: Fails due to missing FUSE OP (46)
echo "generic/286" >> xfs_excludes.txt
# TODO: Hangs
echo "generic/430" >> xfs_excludes.txt
echo "generic/431" >> xfs_excludes.txt
echo "generic/434" >> xfs_excludes.txt
echo "generic/478" >> xfs_excludes.txt

# TODO: requires supporting orphaned files, that have an open file handle, but no links
echo "generic/035" >> xfs_excludes.txt

# TODO: Broken. Dunno why
echo "generic/075" >> xfs_excludes.txt
echo "generic/091" >> xfs_excludes.txt
echo "generic/112" >> xfs_excludes.txt
echo "generic/184" >> xfs_excludes.txt
echo "generic/221" >> xfs_excludes.txt
echo "generic/263" >> xfs_excludes.txt
echo "generic/360" >> xfs_excludes.txt
echo "generic/394" >> xfs_excludes.txt
echo "generic/423" >> xfs_excludes.txt
echo "generic/426" >> xfs_excludes.txt
echo "generic/467" >> xfs_excludes.txt
echo "generic/469" >> xfs_excludes.txt
echo "generic/477" >> xfs_excludes.txt
echo "generic/484" >> xfs_excludes.txt
echo "generic/504" >> xfs_excludes.txt

FLEETFS_EXTRA_MOUNT_OPTIONS="" TEST_SERVER="127.0.0.1:3300" SCRATCH_SERVER="127.0.0.1:3400" \
./check-fleetfs -g quick -E xfs_excludes.txt \
| tee /code/logs/xfstests.log

export XFSTESTS_EXIT_STATUS=${PIPESTATUS[0]}

rm -rf ${DATA_DIR}
rm -rf ${DATA_DIR2}
rm -rf ${SCRATCH_DIR}
rm -rf ${SCRATCH_DIR2}
