from typing import List
from typing import Union

import pystac
from gcsfs import GCSFileSystem
from satextractor.models import ExtractionTask
from satextractor.models import Tile
from satextractor.scheduler import create_tasks_by_splits


def get_scheduler(name, **kwargs):
    return eval(name)


def gcp_schedule(
    tiles: List[Tile],
    split_m: int,
    item_collection: Union[str, pystac.ItemCollection],
    constellations: List[str],
    bands: List[str] = None,
    interval: int = 1,
    n_jobs: int = -1,
    verbose: int = 0,
    overwrite: bool = False,
    storage_path: str = None,
    credentials=None,
    **kwargs,
) -> List[ExtractionTask]:

    fs = GCSFileSystem(token=credentials)
    return create_tasks_by_splits(
        tiles,
        split_m,
        item_collection,
        constellations,
        bands,
        interval,
        n_jobs,
        verbose,
        overwrite,
        storage_path,
        fs.get_mapper,
    )
