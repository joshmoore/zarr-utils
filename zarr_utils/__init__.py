"""CLI tools for working with Zarr"""

__version__ = '0.1'

import click
from rich.progress import track

# https://github.com/willmcgugan/rich/blob/master/examples/downloader.py
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os.path
import sys
from typing import Iterable
from urllib.request import urlopen

from rich.progress import (
    BarColumn,
    DownloadColumn,
    TextColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    Progress,
    TaskID,
)
progress = Progress(
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
)

def copy_url(task_id: TaskID, url: str, path: str) -> None:
    """Copy data from a url to a local file."""
    response = urlopen(url)
    # This will break if the response doesn't contain content length
    progress.update(task_id, total=int(response.info()["Content-length"]))
    with open(path, "wb") as dest_file:
        progress.start_task(task_id)
        for data in iter(partial(response.read, 32768), b""):
            dest_file.write(data)
            progress.update(task_id, advance=len(data))


@click.command()
@click.argument("source")
@click.argument("target")
def _zarr2hdf(source, target):
    with ThreadPoolExecutor(max_workers=4) as pool:
        for url in (source, target):
            filename = url.split("/")[-1]
            dest_path = os.path.join(dest_dir, filename)
            task_id = progress.add_task("download", filename=filename, start=False)
            pool.submit(copy_url, task_id, url, dest_path)
    for step in track(range(100)):
        pass
    print(source, target)


import os
import h5py
import zarr

@click.command()
@click.argument("source")
@click.argument("target")
def hdf2zarr(source, target):

    opened = False
    if isinstance(source, (bytes, str)):
        hdf5_filename = source
        hdf5_file = h5py.File(source, "r")
        opened = True
    else:
        hdf5_file = source
        hdf5_filename = hdf5_file.filename

    store = zarr.DirectoryStore(target)
    root = zarr.group(store=store, overwrite=True)

    def copy(name, obj):
        if isinstance(obj, h5py.Group):
            zarr_obj = root.create_group(name)
        elif isinstance(obj, h5py.Dataset):
            zarr_obj = root.create_dataset(name, data=obj, chunks=obj.chunks)
        else:
            assert False, "Unsupport HDF5 type."

        zarr_obj.attrs.update(obj.attrs)

    hdf5_file.visititems(copy)

    if opened:
        hdf5_file.close()

    return root
