import requests
from requests.exceptions import HTTPError


def _validate_host(host: str) -> str:
    # Host must begin with https://
    if host[:8] != "https://":
        host = f"https://{host}"

    # Host must NOT end with a /
    if host[-1:] == "/":
        host = host[:-1]
    return host


def trigger_job_run(job_id: int, host: str, http_header: dict) -> int:

    host = _validate_host(host)
    req = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=http_header,
        json={
            "job_id": job_id,
        },
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json().get("run_id")


def get_job_run(run_id: int, host: str, http_header: dict) -> dict:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.1/jobs/runs/get",
        headers=http_header,
        json={"run_id": run_id},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def get_job(job_id: int, host: str, http_header: dict) -> dict:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.1/jobs/get?job_id={job_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def update_dbt_job(
    job_id: int,
    job_cluster_id: str,
    use_case: str,
    cluster_size: str,
    serverless: bool,
    incremental: bool,
    run: int,
    host: str,
    http_header: dict,
):
    component = "job"
    benchmark_config = (
        "dbt__"
        + component
        + "__"
        + cluster_size.lower()
        + ("__serverless" if serverless else "__server-based")
    )

    # Updating run command to set full-refresh flag if needed and dbt-vars in task to
    # have a table prefix per run
    job = get_job(job_id, host, http_header)

    tasks = job["settings"]["tasks"]
    tasks[1]["python_wheel_task"]["parameters"][0] = "run --select tag:example" + (
        " --full-refresh" if not incremental else ""
    )
    tasks[1]["python_wheel_task"]["parameters"][1] = (
        '--vars {"prefix_table":true,"prefix":"'
        + cluster_size.lower()
        + ("__serverless" if serverless else "__server-based")
        + "__run_"
        + str(run)
        + '_"}'
    )

    tasks[0]["existing_cluster_id"] = job_cluster_id
    tasks[1]["existing_cluster_id"] = job_cluster_id

    # Build the custom tags
    custom_tags = {
        "benchmark_config_run": benchmark_config + "__run_" + str(run),
        "benchmark_config": benchmark_config,
        "benchmark_run_nr": str(run),
        "benchmark_use_case": use_case,
        "benchmark_component": component,
    }

    new_settings = {"tags": custom_tags, "tasks": tasks}

    update_job(job_id, new_settings, host, http_header)


def update_dlt_job(
    job_id: int,
    pipeline_id: str,
    use_case: str,
    nr_of_workers: int,
    photon: bool,
    incremental: bool,
    run: int,
    host: str,
    http_header: dict,
):
    component = "job"
    benchmark_config = (
        "dlt__"
        + component
        + "__"
        + str(nr_of_workers)
        + "_workers"
        + ("__photon" if photon else "__no_photon")
    )

    # Updating pipeline_id and 'full_refresh' checkbox
    job = get_job(job_id, host, http_header)

    tasks = job["settings"]["tasks"]
    tasks[0]["pipeline_task"]["pipeline_id"] = pipeline_id
    tasks[0]["pipeline_task"]["full_refresh"] = not incremental

    # Build the custom tags
    custom_tags = {
        "benchmark_config_run": benchmark_config + "__run_" + str(run),
        "benchmark_config": benchmark_config,
        "benchmark_run_nr": str(run),
        "benchmark_use_case": use_case,
        "benchmark_component": component,
    }

    new_settings = {"tags": custom_tags, "tasks": tasks}

    update_job(job_id, new_settings, host, http_header)


def update_job(job_id: int, new_settings: dict, host: str, http_header: dict):
    host = _validate_host(host)

    req = requests.post(
        f"{host}/api/2.0/jobs/update",
        headers=http_header,
        json={"job_id": job_id, "new_settings": new_settings},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
