from datetime import datetime
from typing import Dict
from typing import List

import numpy as np
import zarr
from gcsfs import GCSFileSystem
from joblib import delayed
from joblib import Parallel
from loguru import logger
from satextractor.models import ExtractionTask
from satextractor.models import Tile
from satextractor.models.constellation_info import BAND_INFO
from satextractor.preparer import create_zarr_patch_structure
from satextractor.utils import tqdm_joblib
from tqdm import tqdm


def gcp_prepare_archive(
    credentials: str,
    tasks: List[ExtractionTask],
    tiles: List[Tile],
    constellations: List[str],
    storage_root: str,
    patch_size: int,
    chunk_size: int,
    n_jobs: int = -1,
    verbose: int = 0,
    **kwargs,
) -> bool:
    fs = GCSFileSystem(token=credentials)
    # make a dict of tiles and constellations sensing times
    tile_constellation_sensing_times: Dict[str, Dict[str, List[datetime]]] = {
        tt.id: {kk: [] for kk in BAND_INFO.keys() if kk in constellations}
        for tt in tiles
    }

    for task in tasks:

        # check tiles meet spec
        assert isinstance(
            task,
            ExtractionTask,
        ), "Task does not match ExtractionTask spec"

        for tile in task.tiles:
            tile_constellation_sensing_times[tile.id][task.constellation].append(
                task.sensing_time,
            )

    # get the unique sensing times
    for tt in tiles:
        for kk in constellations:
            tile_constellation_sensing_times[tt.id][kk] = np.array(
                [
                    np.datetime64(el)
                    for el in sorted(
                        list(set(tile_constellation_sensing_times[tt.id][kk])),
                    )
                ],
            )

    items = tile_constellation_sensing_times.items()
    with tqdm_joblib(
        tqdm(
            desc=f"parallel building zarr tile roots on {storage_root}",
            total=len(items),
        ),
    ):
        Parallel(n_jobs=n_jobs, verbose=verbose, prefer="threads")(
            [
                delayed(zarr.open)(fs.get_mapper(f"{storage_root}/{tile_id}"))
                for tile_id, _ in items
            ],
        )

    logger.info(f"parallel building zarr archives on {storage_root}")
    jobs = []
    for tile_id, vv in items:
        for constellation, sensing_times in vv.items():
            jobs.append(
                delayed(create_zarr_patch_structure)(
                    fs.get_mapper,
                    storage_root,
                    tile_id,
                    patch_size,
                    chunk_size,
                    sensing_times,
                    constellation,
                    BAND_INFO[constellation],
                ),
            )

    with tqdm_joblib(
        tqdm(desc="Building Archives.", total=len(tiles) * len(constellations)),
    ):
        Parallel(n_jobs=n_jobs, verbose=verbose, prefer="threads")(jobs)

    return True
