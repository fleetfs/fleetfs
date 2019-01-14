import sys

from fuse import FUSE, FuseOSError, Operations


class FleetFUSE(Operations):
    def __init__(self, storage_path):
        self.storage_path = storage_path


def main(mountpoint, storage_path):
    FUSE(FleetFUSE(storage_path), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
