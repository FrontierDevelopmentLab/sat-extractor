import concurrent
import hashlib
import json
from datetime import datetime

from google.cloud import pubsub_v1
from loguru import logger
from satextractor.models.constellation_info import BAND_INFO
from tqdm import tqdm


def deploy_tasks(credentials, extraction_tasks, storage_path, chunk_size, topic):

    user_id = topic.split("/")[-1].split("-")[0]

    job_id = hashlib.sha224(
        (user_id + str(datetime.now())).encode(),
    ).hexdigest()[:10]

    logger.info(f"Deploying {len(extraction_tasks)} tasks with job_id: {job_id}")

    publisher = pubsub_v1.PublisherClient.from_service_account_json(credentials)
    publish_futures = []
    for i, task in tqdm(enumerate(extraction_tasks)):
        extraction_task_data = task.serialize()
        data = dict(
            storage_gs_path=storage_path,
            job_id=job_id,
            extraction_task=extraction_task_data,
            bands=list(BAND_INFO[task.constellation].keys()),
            chunks=(1, 1, chunk_size, chunk_size),
        )
        data = json.dumps(data, default=str)

        publish_future = publisher.publish(topic, data.encode("utf-8"))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    concurrent.futures.wait(
        publish_futures,
        return_when=concurrent.futures.ALL_COMPLETED,
    )

    logger.info("Done publishing tasks!")

    return job_id
