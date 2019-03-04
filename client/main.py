import errno
import logging

import requests
import stat
import sys

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


PATH_HEADER = 'X-Fleet-Path'


class FleetFUSE(LoggingMixIn, Operations):
    def __init__(self, server_url):
        self.server_url = server_url

    def getattr(self, path, fh=None):
        if path == '/':
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        files = self.readdir('/', fh=(0,))
        if path.lstrip('/') in files:
            r = requests.get(self.server_url, headers={PATH_HEADER: path})
            if r.status_code != 200:
                raise FuseOSError(errno.EIO)
            return dict(
                st_mode=(stat.S_IFREG | 0o777),
                st_nlink=1,
                st_uid=0,
                st_gid=0,
                st_size=len(r.content),
                st_atime=0,
                st_mtime=0,
                st_ctime=0
            )
        raise FuseOSError(errno.ENOENT)

    def read(self, path, size, offset, fh):
        r = requests.get(self.server_url, headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        content = r.content
        return content[offset:size]

    def readdir(self, path, fh):
        r = requests.get(self.server_url, headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return r.json()

    def truncate(self, path, length, fh=None):
        if length != 0:
            raise FuseOSError(errno.EIO)
        r = requests.post(self.server_url + '/truncate', headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return 0

    def create(self, path, mode, fi=None):
        self.write(path, '', 0, fh=0)

    def write(self, path, data, offset, fh):
        r = requests.post(self.server_url + '/' + str(offset), data, headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return len(data)


def main(mountpoint, server_url):
    FUSE(FleetFUSE(server_url), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv[2], sys.argv[1])
