from typing import Optional

import requests
from helpers.databricks.utils import validate_host
from requests.exceptions import HTTPError


def get_job_id(job_name: str, host: str, http_header: dict) -> Optional[int]:
    """Get the job id of a provide job name.
    :param job_name: the name of the job
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    host = validate_host(host)
    req = requests.get(
        f"{host}/api/2.1/jobs/list?name={job_name}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    # When filtered on name, the result will be 0 or 1 jobs.
    jobs = req.json().get("jobs")
    if jobs:
        return jobs[0].get("job_id")


def start_job(job_id: str, queue: bool, host: str, http_header: dict) -> dict:
    """Start the job with the specified id
    :param job_id: id of the job to start
    :param queue: boolean indicating if the run should be queued if it already running
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    host = validate_host(host)
    req = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=http_header,
        json={"job_id": job_id, "queue": {"enabled": queue}},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()
