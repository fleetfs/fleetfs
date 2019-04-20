import errno
import logging

import click
import requests
import stat

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


PATH_HEADER = 'X-FleetFS-Path'
OFFSET_HEADER = 'X-FleetFS-Offset'
SIZE_HEADER = 'X-FleetFS-Size'


class FleetFS(LoggingMixIn, Operations):
    def __init__(self, server_url):
        self.server_url = server_url

    def getattr(self, path, fh=None):
        if path == '/':
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        files = self.readdir('/', fh=(0,))
        if path.lstrip('/') in files:
            r = requests.get(self.server_url + '/getattr', headers={PATH_HEADER: path})
            if r.status_code != 200:
                raise FuseOSError(errno.EIO)
            metadata = r.json()
            return dict(
                st_mode=(stat.S_IFREG | 0o777),
                st_nlink=1,
                st_uid=0,
                st_gid=0,
                st_size=metadata['length'],
                st_atime=0,
                st_mtime=0,
                st_ctime=0
            )
        raise FuseOSError(errno.ENOENT)

    def read(self, path, size, offset, fh):
        r = requests.get(self.server_url, headers={PATH_HEADER: path, OFFSET_HEADER: str(offset), SIZE_HEADER: str(size)})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        content = r.content
        return content

    def readdir(self, path, fh):
        r = requests.get(self.server_url, headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return r.json()

    def truncate(self, path, length, fh=None):
        r = requests.post(self.server_url + '/truncate/' + str(length), headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return 0

    def create(self, path, mode, fi=None):
        self.write(path, '', 0, fh=0)
        return 0

    def write(self, path, data, offset, fh):
        r = requests.post(self.server_url + '/' + str(offset), data, headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return len(data)

    def unlink(self, path):
        if path == '/':
            raise FuseOSError(errno.EROFS)
        r = requests.delete(self.server_url, headers={PATH_HEADER: path})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)
        return 0


VERBOSITY = [logging.FATAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]


@click.command()
@click.option("--server-url", default="http://localhost:3000", help="URL of server")
@click.option("--mount-point", required=True, help="Local mount point for the filesystem")
@click.option("--direct-io", is_flag=True, help="Mount filesystem with direct IO")
@click.option('-v', '--verbose', count=True)
def main(server_url, mount_point, direct_io, verbose):
    logging.basicConfig(level=logging.DEBUG if verbose >= len(VERBOSITY) else VERBOSITY[verbose])
    FUSE(FleetFS(server_url), mount_point, nothreads=True, foreground=True, direct_io=direct_io)


if __name__ == '__main__':
    main()
