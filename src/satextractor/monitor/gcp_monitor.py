from datetime import datetime

from google.cloud import bigquery
from satextractor.monitor.base import BaseMonitor


class GCPMonitor(BaseMonitor):
    def __init__(
        self,
        table_name: str,
        storage_path: str,
        job_id: str,
        task_id: str,
        constellation: str,
    ):

        self.client = bigquery.Client()
        self.table_name = table_name
        self.storage_path = storage_path
        self.job_id = job_id
        self.task_id = task_id
        self.constellation = constellation
        self.dataset_name = storage_path.split("/")[-1]

    def post_status(
        self,
        msg_type: str,
        msg_payload: str,
    ) -> bool:

        # CLOUD FUNCTION CANNOT INSERT ROWS, ONLY UPDATE STATUS
        msg_types = ["STARTED", "FINISHED", "FAILED"]

        assert (
            msg_type in msg_types
        ), f"msg_type '{msg_type}' not allowed. msg_type must be in '{msg_types}' "

        vals = {
            "job_id": self.job_id,
            "task_id": self.task_id,
            "storage_gs_path": self.storage_path,
            "msg_type": msg_type,
            "msg_payload": msg_payload,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "dataset_name": self.dataset_name,
            "constellation": self.constellation,
        }

        errors = self.client.insert_rows_json(self.table_name, [vals])
        if errors != []:
            raise ValueError(
                f"there where {len(errors)} error when inserting. " + str(errors),
            )

        return True
