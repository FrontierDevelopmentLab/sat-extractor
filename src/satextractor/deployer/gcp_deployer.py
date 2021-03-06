import concurrent
import json

from google.api_core import retry
from google.auth import jwt
from google.cloud import pubsub_v1
from loguru import logger
from satextractor.models.constellation_info import BAND_INFO
from tqdm import tqdm


def deploy_tasks(
    job_id,
    credentials,
    extraction_tasks,
    storage_path,
    chunk_size,
    topic,
):

    logger.info(f"Deploying {len(extraction_tasks)} tasks with job_id: {job_id}")

    credentials_json = json.load(open(credentials, "r"))

    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    credentials_ob = jwt.Credentials.from_service_account_info(
        credentials_json,
        audience=audience,
    )

    publisher = pubsub_v1.PublisherClient(credentials=credentials_ob)

    short_retry = retry.Retry(deadline=60)

    publish_futures = []

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
