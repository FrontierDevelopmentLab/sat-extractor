"""
Build the cloud functions.
1. Package the functions in zip and write them to the bucket
2. Deploy using function with the bucket as source
hlpful: https://stackoverflow.com/questions/47376380/create-google-cloud-function-using-api-in-python
"""
import json
import subprocess
from subprocess import run

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import pubsub_v1 as pubsub
from loguru import logger


def build_gcp(cfg):

    builder = BuildGCP(
        credentials=cfg.credentials,
        **cfg.cloud,
        **cfg.builder,
    )

    builder.build()

    return 1


class BuildGCP:
    def __init__(
        self,
        project,
        region,
        storage_root,
        credentials,
        user_id,
        **kwargs,
    ):

        self.project = project
        self.region = region
        self.dest_bucket = storage_root.split("/")[0]
        self.gcp_credentials = credentials
        self.user_id = user_id

        if "europe" in self.region:
            self.image_region_code = "eu.gcr.io"
        elif "america" in self.region or self.region.split("-") == "us":
            self.image_region_code = "us.gcr.io"
        elif "asia" in self.region or "australia" in self.region:
            self.image_region_code = "asia.gcr.io"
        else:
            self.image_region_code = "gcr.io"

    def build(self):

        logger.info("building docker image")
        self.build_docker_image()
        logger.info("building pubsub topic")
        self.build_pubsub_topic()
        logger.info("building tracking tables")
        self.build_tracking_tables()
        logger.info("building cloud run service")
        self.build_cloudrun_service()

        return 1

    def bq_dataset_exists(self, client, name):
        try:
            client.get_dataset(name)
            return True
        except NotFound:
            return False

    def bq_table_exists(self, client, name):
        try:
            client.get_table(name)
            return True
        except NotFound:
            return False

    def build_tracking_tables(self):

        bq_client = bigquery.Client.from_service_account_json(self.gcp_credentials)

        logger.info("Creating bigquery table to monitor tasks")

        # check if dataset exists
        task_tracking_dataset = ".".join(
            [
                self.project,
                "satextractor",
            ],
        )

        task_tracking_table = ".".join(
            [
                self.project,
                "satextractor",
                self.user_id,
            ],
        )
        self.task_tracking_table = task_tracking_table

        if not self.bq_dataset_exists(bq_client, task_tracking_dataset):
            logger.info(f"Creating Dataset {task_tracking_dataset}")
            dataset = bigquery.Dataset(task_tracking_dataset)
            bq_client.create_dataset(dataset)
            logger.info("Created Dataset")

        if not self.bq_table_exists(bq_client, task_tracking_table):
            logger.info(f"Creating Table {task_tracking_table}")

            schema = [
                bigquery.SchemaField("timestamp", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("storage_gs_path", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("msg_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("msg_payload", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("constellation", "STRING", mode="REQUIRED"),
            ]

            table = bigquery.Table(task_tracking_table, schema=schema)
            table = bq_client.create_table(table)  # Make an API request.
            logger.info(
                f"created table {table.project}.{table.dataset_id}.{table.table_id}",
            )

        return 1

    def build_docker_image(self):
        logger.info("Building docker image...")

        cmd = [
            "gcloud",
            "builds",
            "submit",
            "--tag",
            f"{self.image_region_code}/{self.project}/{self.user_id}-stacextractor",
        ]

        p = run(cmd, text=True, stdout=subprocess.DEVNULL)
        p.check_returncode()

        return 1

    def build_pubsub_topic(self):

        # build pubsub
        publisher = pubsub.PublisherClient.from_service_account_file(
            self.gcp_credentials,
        )

        topic_name = "projects/{project_id}/topics/{topic}".format(
            project_id=self.project,
            topic="-".join([self.user_id, "stacextractor"]),
        )

        dlq_topic_name = "projects/{project_id}/topics/{topic}".format(
            project_id=self.project,
            topic="-".join([self.user_id, "stacextractor-dql"]),
        )

        self.topic_name = topic_name
        self.dlq_topic_name = dlq_topic_name

        # check if topic exists
        existing_topics = publisher.list_topics(project=f"projects/{self.project}")
        existing_topic_names = [t.name for t in existing_topics]

        if topic_name not in existing_topic_names:
            publisher.create_topic(name=topic_name)

        if dlq_topic_name not in existing_topic_names:
            publisher.create_topic(name=dlq_topic_name)

        return 1

    def build_cloudrun_service(self):

        # https://cloud.google.com/run/docs/tutorials/pubsub

        # deploy the image
        # gcloud run deploy pubsub-tutorial --image gcr.io/PROJECT_ID/pubsub  --no-allow-unauthenticated

        logger.info("deploying image")

        cmd = [
            "gcloud",
            "run",
            "deploy",
            f"{self.user_id}-stacextractor",
            "--region",
            f"{self.region}",
            "--image",
            f"{self.image_region_code}/{self.project}/{self.user_id}-stacextractor",
            "--update-env-vars",
            f"MONITOR_TABLE={self.task_tracking_table}",
            "--no-allow-unauthenticated",
            "--memory",
            "4G",
            "--timeout",
            "15m",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        # subscribe the credentialed service account to the pubsub topic

        # gcloud run services add-iam-policy-binding pubsub-tutorial \
        # --member=serviceAccount:cloud-run-pubsub-invoker@PROJECT_ID.iam.gserviceaccount.com \
        # --role=roles/run.invoker

        service_account_email = json.load(open(self.gcp_credentials, "r"))[
            "client_email"
        ]

        logger.info("binding service account")

        cmd = [
            "gcloud",
            "run",
            "services",
            "add-iam-policy-binding",
            f"{self.user_id}-stacextractor",
            f"--member=serviceAccount:{service_account_email}",
            "--role=roles/run.invoker",
            "--region",
            f"{self.region}",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        # get the service endpoint url

        logger.info("get service endpoint url")

        cmd = [
            "gcloud",
            "run",
            "services",
            "describe",
            f"{self.user_id}-stacextractor",
            "--platform",
            "managed",
            "--region",
            f"{self.region}",
            "--format",
            "value(status.url)",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        url = p.stdout.strip()
        print(url)

        logger.info("bind the topic to the endpoint")

        cmd = [
            "gcloud",
            "pubsub",
            "subscriptions",
            "create",
            f"{self.user_id}-stacextractor",
            "--topic",
            f"{self.topic_name}",
            f"--push-endpoint={url}/",
            f"--push-auth-service-account={service_account_email}",
            "--ack-deadline",
            "600",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        logger.info("adding deadletter")

        cmd = [
            "gcloud",
            "pubsub",
            "subscriptions",
            "update",
            f"{self.user_id}-stacextractor",
            "--dead-letter-topic",
            f"{self.dlq_topic_name}",
            "--max-delivery-attempts",
            "5",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        logger.info("adding deadletter permissions")
        cmd = [
            "gcloud",
            "pubsub",
            "topics",
            "add-iam-policy-binding",
            f"{self.dlq_topic_name}",
            f"--member=serviceAccount:{service_account_email}",
            "--role=roles/pubsub.publisher",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        cmd = [
            "gcloud",
            "pubsub",
            "subscriptions",
            "add-iam-policy-binding",
            f"{self.user_id}-stacextractor",
            f"--member=serviceAccount:{service_account_email}",
            "--role=roles/pubsub.subscriber",
        ]

        p = run(cmd, capture_output=True, text=True)
        print(p.stdout)
        print(p.stderr)

        return 1
