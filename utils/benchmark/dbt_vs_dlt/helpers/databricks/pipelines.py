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


def get_pipeline(pipeline_id: str, host: str, http_header: dict) -> dict:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def find_pipelines(pipeline_name: str, host: str, http_header: dict) -> str:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.0/pipelines",
        headers=http_header,
        json={"filter": "name like '" + pipeline_name + "'"},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    # When filtered on name, the result will be 0 or 1 jobs.
    pipelines = req.json().get("statuses")
    if pipelines:
        return pipelines[0].get("pipeline_id")


def get_pipeline_update(
    pipeline_id: str, update_id: str, host: str, http_header: dict
) -> dict:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def create_or_update_pipeline(
    template_pipeline: dict,
    use_case: str,
    nr_of_workers: int,
    photon: bool,
    run: int,
    host: str,
    http_header: dict,
) -> str:

    component = "dlt_pipeline"
    benchmark_config = (
        "dlt__"
        + component
        + "__"
        + str(nr_of_workers)
        + "_workers"
        + ("__photon" if photon else "__no_photon")
    )

    pipeline_name = (
        "benchmark_example_data__"
        + str(nr_of_workers)
        + "_workers"
        + ("__photon" if photon else "__no_photon")
        + "__run_"
        + str(run)
    )

    # check if pipeline already exists
    pipeline_id = find_pipelines(pipeline_name, host, http_header)

    # Getting required properties from template pipeline
    cluster = template_pipeline.get("spec").get("clusters")[0]
    cluster["autoscale"]["min_workers"] = nr_of_workers
    cluster["autoscale"]["max_workers"] = nr_of_workers

    # Update the custom tags
    custom_tags = cluster.get("custom_tags") if cluster.get("custom_tags") else {}
    custom_tags.update(
        {
            "benchmark_config_run": benchmark_config + "__run_" + str(run),
            "benchmark_config": benchmark_config,
            "benchmark_run_nr": str(run),
            "benchmark_use_case": use_case,
            "benchmark_component": component,
        }
    )
    cluster.update({"custom_tags": custom_tags})

    configuration = {
        "table_prefix": str(nr_of_workers)
        + "_workers"
        + ("__photon" if photon else "__no_photon")
        + "__run_"
        + str(run)
        + "_",
        "pipelines.clusterShutdown.delay": "60s",
    }

    body = {
        "name": pipeline_name,
        "target": template_pipeline.get("spec").get("target"),
        "catalog": template_pipeline.get("spec").get("catalog"),
        "clusters": cluster,
        "libraries": template_pipeline.get("spec").get("libraries"),
        "photon": photon,
        "development": True,
        "edition": template_pipeline.get("spec").get("edition"),
        "channel": template_pipeline.get("spec").get("channel"),
        "configuration": configuration,
    }

    if pipeline_id:
        print(f"DLT: updating existing pipeline '{pipeline_id}'")
        update_pipeline(pipeline_id, body, host, http_header)
    else:
        print("DLT: creating new pipeline")
        pipeline_id = create_pipeline(body, host, http_header)

    return pipeline_id


def create_pipeline(body: dict, host: str, http_header: dict) -> str:

    host = _validate_host(host)
    req = requests.post(
        f"{host}/api/2.0/pipelines",
        headers=http_header,
        json=body,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json().get("pipeline_id")


def update_pipeline(pipeline_id: str, body: dict, host: str, http_header: dict):

    host = _validate_host(host)
    req = requests.put(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=http_header,
        json=body,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def validate_pipeline(pipeline_id: str, host: str, http_header: dict) -> str:

    host = _validate_host(host)
    req = requests.post(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
        headers=http_header,
        json={"validate_only": True},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json().get("update_id")


def stop_pipeline(pipeline_id: str, host: str, http_header: dict) -> str:

    host = _validate_host(host)
    req = requests.post(
        f"{host}/api/2.0/pipelines/{pipeline_id}/stop",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
