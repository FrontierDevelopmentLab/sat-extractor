from typing import List
import datetime

import cattr
import gcsfs
import pystac
from loguru import logger
from satextractor.extractor import task_mosaic_patches
from satextractor.models import BAND_INFO
from satextractor.models import ExtractionTask
from satextractor.models import Tile
from satextractor.storer import store_patches


def extract_patches(
    extraction_task: ExtractionTask,
    storage_gs_path: str,
    job_id: int,
    bands: List[str],
) -> int:
    fs = gcsfs.GCSFileSystem()
    tiles = [cattr.structure(t, Tile) for t in extraction_task["tiles"]]
    item_collection = pystac.ItemCollection.from_dict(
        extraction_task["item_collection"],
    )
    band = extraction_task["band"]
    task_id = extraction_task["task_id"]
    constellation = extraction_task["constellation"]
    sensing_time = datetime.datetime.fromisoformat(extraction_task["sensing_time"])
    task = ExtractionTask(
        task_id,
        tiles,
        item_collection,
        band,
        constellation,
        sensing_time,
    )

    logger.info(f"Ready to extract {len(task.tiles)} tiles.")

    archive_resolution = int(
        min([b["gsd"] for _, b in BAND_INFO[constellation].items()]),
    )

    patches = task_mosaic_patches(
        cloud_fs=fs,
        task=task,
        method="max",
        resolution=archive_resolution,
    )

    logger.info(f"Ready to store {len(patches)} patches at {storage_gs_path}.")
    store_patches(
        fs.get_mapper,
        storage_gs_path,
        patches,
        task,
        bands,
        archive_resolution,
    )

    logger.info(
        f"{len(patches)} patches were succesfully stored in {storage_gs_path}.",
    )

    return len(patches)
