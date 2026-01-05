import gzip
import hashlib
import logging
import os
import tarfile
from pathlib import Path
from typing import Optional, Iterable

logger = logging.getLogger(__name__)

def reset(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
    """
    Reset tarinfo metadata to ensure reproducible archives.
    """
    tarinfo.uid = 0
    tarinfo.gid = 0
    tarinfo.uname = "root"
    tarinfo.gname = "root"
    tarinfo.mtime = 0  # TODO: clamp time instead of zeroing
    
    return tarinfo


def make_targz(
    items: Iterable[Path], 
    dest_name: Optional[str] = None,
    working_directory: Optional[str] = os.getcwd(),
    archive_root: Optional[str] = None
) -> str:
    """
    Make a reproducible (no mtime) targz (compressed) archive from a source directory.
    """
    from oras.utils import get_tmpfile

    dest_name = dest_name or get_tmpfile(suffix=".tar.gz")

    # os.O_WRONLY tells the computer you are only going to writo to the file, not read
    # os.O_CREATE tells the computer to create the file if it doesn't exist
    with os.fdopen(
            os.open(dest_name, os.O_WRONLY | os.O_CREAT, 0o644), "wb"
    ) as out_file:
        with gzip.GzipFile(mode="wb", fileobj=out_file, mtime=0) as gzip_file:
            with tarfile.open(fileobj=gzip_file, mode="w:") as tar_file:
                for item in items:
                    logger.debug("Adding %s to archive %s", item, dest_name)
                    tar_file.add(
                        os.path.join(working_directory, item),
                        filter=reset,
                        arcname=os.path.join(archive_root or "", item)
                    )

    return dest_name


def diff_id_from_tar_gz(tar_gz_path: str) -> str:
    """
    Calculate the diff ID (SHA256 hash) of the uncompressed tar file.
    """
    with open(tar_gz_path, "rb") as tar_gz_file:
        with gzip.GzipFile(fileobj=tar_gz_file, mode="rb") as gzip_file:
             return hashlib.file_digest(gzip_file, "sha256").hexdigest()
        
        
if __name__ == "__main__":
    make_targz(
        "/Users/brendan/Documents/PycharmProjects/oci-tools/src/prefect_oci/hack/deps",
        "/Users/brendan/Documents/PycharmProjects/oci-tools/src/prefect_oci/hack/deps.tgz"
    )

    make_targz(
        "/Users/brendan/Documents/PycharmProjects/oci-tools/src/prefect_oci/hack/req",
        "/Users/brendan/Documents/PycharmProjects/oci-tools/src/prefect_oci/hack/code.tgz"
    )