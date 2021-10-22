import datetime
from typing import Any
from typing import List

import numpy as np
import zarr
from satextractor.models import ExtractionTask
from scipy.ndimage import zoom


def store_patches(
    fs_mapper: Any,
    storage_path: str,
    patches: List[np.ndarray],
    task: ExtractionTask,
    bands: List[str],
    patch_resolution: int,
    archive_resolution: int,
):
    """Store a list of patches in storage path.
    Assumes the target structure file is already created.

    Args:
        fs_mapper (Any): a file system mapper to map the path, e.x: gcsfs.get_mapper
        storage_path (str): The path where to store the patches
        patches (List[np.ndarray]): The patches as numpy arrays
        task (ExtractionTask): The extraction task containing the tiles
        bands (List[str]): the bands
    """
    tiles = task.tiles
    for i, tile in enumerate(tiles):

        data_path = f"{storage_path}/{tile.id}/{task.constellation}/data"
        timestamps_path = f"{storage_path}/{tile.id}/{task.constellation}/timestamps"

        timestamps = zarr.open(fs_mapper(timestamps_path))[:]
        timestamps = [datetime.datetime.fromisoformat(el) for el in timestamps]

        size = (
            tile.bbox_size[0] // archive_resolution,
            tile.bbox_size[1] // archive_resolution,
        )
        arr = zarr.open_array(
            store=fs_mapper(data_path),
            dtype=np.uint16,
        )
        band_idx = bands.index(task.band.upper())
        timestamp_idx = timestamps.index(task.sensing_time)
        patch = patches[i]

        # maybe resize -> bicubic upsample
        if patch_resolution != archive_resolution:
            patch = zoom(patch, int(patch_resolution / archive_resolution), order=3)

        # in patch resolution
        if patch.shape != size:
            pad_x = int(size[0] - patch.shape[0])
            pad_y = int(size[1] - patch.shape[1])
            patch = np.pad(patch, [(0, pad_x), (0, pad_y)])
        assert patch.shape == size
        arr[timestamp_idx, band_idx, :, :] = patch
