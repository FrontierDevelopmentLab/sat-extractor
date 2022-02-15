import concurrent
import datetime
import json
from collections import defaultdict

import numpy as np
import zarr
from gcsfs import GCSFileSystem
from google.api_core import retry
from google.auth import jwt
from google.cloud import pubsub_v1
from loguru import logger
from satextractor.models.constellation_info import BAND_INFO
from tqdm import tqdm


def filter_already_extracted_tasks(fs_mapper, storage_path, extraction_tasks):

    tiles = set([tile.id for task in extraction_tasks for tile in task.tiles])
    constellations = set([tile.id for task in extraction_tasks for tile in task.tiles])

    tile_constellation_sensing_times = defaultdict(np.array)

    # Get the existing dates for the task tiles and constellation
    for tile_id in tiles:
        for constellation in constellations:
            patch_constellation_path = f"{storage_path}/{tile_id}/{constellation}"

            timestamps_path = f"{patch_constellation_path}/timestamps"

            existing_timestamps = zarr.open_array(fs_mapper(timestamps_path), "r")[:]
            existing_timestamps = np.array(
                [
                    np.datetime64(datetime.datetime.fromisoformat(el))
                    for el in existing_timestamps
                ],
            )

            tile_constellation_sensing_times[
                patch_constellation_path
            ] = existing_timestamps

    non_extracted = []
    for task in extraction_tasks:
        first_tile = task.tiles[0]  # we only need one for this check
        patch_constellation_path = (
            f"{storage_path}/{first_tile.id}/{task.constellation}"
        )
        if (
            task.sensing_time
            not in tile_constellation_sensing_times[patch_constellation_path]
        ):
            non_extracted.append(task)

    return non_extracted


def deploy_tasks(
    job_id,
    credentials,
    extraction_tasks,
    storage_path,
    chunk_size,
    topic,
    overwrite=False,
):

    logger.info(f"Deploying {len(extraction_tasks)} tasks with job_id: {job_id}")

    credentials_json = json.load(open(credentials, "r"))

    fs = GCSFileSystem(token=credentials)

    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    credentials_ob = jwt.Credentials.from_service_account_info(
        credentials_json,
        audience=audience,
    )

    publisher = pubsub_v1.PublisherClient(credentials=credentials_ob)

    short_retry = retry.Retry(deadline=60)

    publish_futures = []

    if not overwrite:

        filtered_extraction_tasks = filter_already_extracted_tasks(
            fs.get_mapper,
            storage_path,
            extraction_tasks,
        )

        logger.info(
            f"{len(extraction_tasks) - len(filtered_extraction_tasks)} tasks were filtered because they already exists in storage",
        )
        extraction_tasks = filtered_extraction_tasks

    for _, task in tqdm(enumerate(extraction_tasks)):
        extraction_task_data = task.serialize()
        data = dict(
            storage_gs_path=storage_path,
            job_id=job_id,
            extraction_task=extraction_task_data,
            bands=list(BAND_INFO[task.constellation].keys()),
            chunks=(1, 1, chunk_size, chunk_size),
        )
        data = json.dumps(data, default=str)

        publish_future = publisher.publish(
            topic,
            data.encode("utf-8"),
            retry=short_retry,
        )
        publish_futures.append(publish_future)

    logger.info(f"Generated {len(publish_futures)} futures.")

    # Wait for all the publish futures to resolve before exiting.
    concurrent.futures.wait(
        publish_futures,
        return_when=concurrent.futures.ALL_COMPLETED,
    )

    logger.info("Done publishing tasks!")

    return job_id
