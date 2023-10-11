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
DATA_DIR3=$(mktemp --directory)
DATA_DIR4=$(mktemp --directory)
DATA_DIR5=$(mktemp --directory)
DATA_DIR6=$(mktemp --directory)

fleetfs --port 3300 --data-dir $DATA_DIR  --redundancy-level 1 --peers 127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon0.log 2>&1 &
fleetfs --port 3301 --data-dir $DATA_DIR2 --redundancy-level 1 --peers 127.0.0.1:3300,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon1.log 2>&1 &
fleetfs --port 3302 --data-dir $DATA_DIR3 --redundancy-level 1 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3303,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon2.log 2>&1 &
fleetfs --port 3303 --data-dir $DATA_DIR4 --redundancy-level 1 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3304,127.0.0.1:3305 > /code/logs/daemon3.log 2>&1 &
fleetfs --port 3304 --data-dir $DATA_DIR5 --redundancy-level 1 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3305 > /code/logs/daemon4.log 2>&1 &
fleetfs --port 3305 --data-dir $DATA_DIR6 --redundancy-level 1 --peers 127.0.0.1:3300,127.0.0.1:3301,127.0.0.1:3302,127.0.0.1:3303,127.0.0.1:3304 > /code/logs/daemon5.log 2>&1 &

SCRATCH_DIR=$(mktemp --directory)
SCRATCH_DIR2=$(mktemp --directory)
fleetfs --port 3400 --data-dir $SCRATCH_DIR --peers 127.0.0.1:3401 > /code/logs/scratch0.log 2>&1 &
fleetfs --port 3401 --data-dir $SCRATCH_DIR2 --peers 127.0.0.1:3400 > /code/logs/scratch1.log 2>&1 &

# Wait for leaders to be elected
until fleetfs --server-ip-port 127.0.0.1:3300 --get-leader; do
    sleep 0.1
done
until fleetfs --server-ip-port 127.0.0.1:3400 --get-leader; do
    sleep 0.1
done

sleep 0.5

set +e
# Clear mount log file, since the tests append to it
echo "" > /code/logs/xfstests_mount.log
DIR=/var/tmp/fuse-xfstests/check-fleetfs
mkdir -p $DIR
cd /code/fuse-xfstests

# TODO: Test 258 doesn't work because libfuse uses u64 type instead of i64 like the Linux kernel uses for timespec.
echo "generic/258" > xfs_excludes.txt

# TODO: requires flock
echo "generic/478" >> xfs_excludes.txt

# TODO: requires supporting orphaned files, that have an open file handle, but no links
echo "generic/035" >> xfs_excludes.txt
echo "generic/426" >> xfs_excludes.txt
echo "generic/467" >> xfs_excludes.txt
echo "generic/477" >> xfs_excludes.txt
echo "generic/484" >> xfs_excludes.txt

# Writes directly to scratch block dev
echo "generic/062" >> xfs_excludes.txt

# TODO: looks like it requires character file support
echo "generic/078" >> xfs_excludes.txt

# TODO: takes > 10min
echo "generic/069" >> xfs_excludes.txt

# TODO: seems like ctime failure
echo "generic/221" >> xfs_excludes.txt

# TODO: needs fallocate
echo "generic/263" >> xfs_excludes.txt

# TODO: Passes, but takes ~30min
echo "generic/127" >> xfs_excludes.txt

# TODO: requires support for mknod on character files
echo "generic/184" >> xfs_excludes.txt
echo "generic/401" >> xfs_excludes.txt

# TODO: requires fifo support
echo "generic/423" >> xfs_excludes.txt

# TODO: requires ulimit support for limiting file size
echo "generic/394" >> xfs_excludes.txt

# TODO: requires lock support
echo "generic/504" >> xfs_excludes.txt

# TODO: requires RENAME_EXCHANGE support
echo "generic/025" >> xfs_excludes.txt

# TODO: requires support for system.posix_acl_access xattr sync'ing to file permissions
# Some information about it linked from here: https://stackoverflow.com/questions/29569408/documentation-of-posix-acl-access-and-friends
echo "generic/099" >> xfs_excludes.txt
echo "generic/105" >> xfs_excludes.txt

# TODO: requires proper suid-bit support
echo "generic/193" >> xfs_excludes.txt
echo "generic/314" >> xfs_excludes.txt
echo "generic/355" >> xfs_excludes.txt
echo "generic/375" >> xfs_excludes.txt
echo "generic/444" >> xfs_excludes.txt

# TODO: requires support for mounting read-only
echo "generic/294" >> xfs_excludes.txt
echo "generic/306" >> xfs_excludes.txt
echo "generic/452" >> xfs_excludes.txt

# TODO: requires atime support
echo "generic/003" >> xfs_excludes.txt
echo "generic/192" >> xfs_excludes.txt

# TODO: Passes, but takes ~10min and writes > 20GB. Needs support for writing files with large holes,
# for this test to be fast
echo "generic/130" >> xfs_excludes.txt

# TODO: uses namespaces and inodes don't seem to get mapped properly
# this test ends up trying to chmod "/" (the root inode)
echo "generic/317" >> xfs_excludes.txt

# TODO: requires more complete ACL support
echo "generic/319" >> xfs_excludes.txt

# TODO: requires supporting non-UTF8 xattr keys
echo "generic/453" >> xfs_excludes.txt
echo "generic/454" >> xfs_excludes.txt

# TODO: Seems to cause a host OOM (even from inside Docker), when run with 84, 87, 88, 100, and 109
echo "generic/089" >> xfs_excludes.txt

# TODO: very slow. Passes, but takes > 30min
echo "generic/074" >> xfs_excludes.txt

# TODO: very slow. Passes, but takes 20min
echo "generic/438" >> xfs_excludes.txt

# TODO: requires COPY_FILE_RANGE support
echo "generic/430" >> xfs_excludes.txt
echo "generic/431" >> xfs_excludes.txt
echo "generic/432" >> xfs_excludes.txt
echo "generic/433" >> xfs_excludes.txt
echo "generic/434" >> xfs_excludes.txt

# TODO: seems to crash host
echo "generic/476" >> xfs_excludes.txt

# TODO: Started failing after switching the Dockerfile to 22.04
echo "generic/286" >> xfs_excludes.txt

# TODO: figure these out
echo "generic/084" >> xfs_excludes.txt
echo "generic/519" >> xfs_excludes.txt
echo "generic/531" >> xfs_excludes.txt
echo "generic/551" >> xfs_excludes.txt
echo "generic/564" >> xfs_excludes.txt
echo "generic/565" >> xfs_excludes.txt

FLEETFS_EXTRA_MOUNT_OPTIONS="" TEST_SERVER="127.0.0.1:3300" SCRATCH_SERVER="127.0.0.1:3400" \
./check-fleetfs -E xfs_excludes.txt \
| tee /code/logs/xfstests.log

export XFSTESTS_EXIT_STATUS=${PIPESTATUS[0]}

if [ $XFSTESTS_EXIT_STATUS ]
then
  ps auxw
  cat /code/logs/*.log
fi

rm -rf ${DATA_DIR}
rm -rf ${DATA_DIR2}
rm -rf ${SCRATCH_DIR}
rm -rf ${SCRATCH_DIR2}
