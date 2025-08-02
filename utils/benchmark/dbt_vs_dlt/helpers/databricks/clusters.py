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


def get_cluster(cluster_id: str, host: str, http_header: dict) -> dict:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.0/clusters/get?cluster_id={cluster_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def start_cluster(cluster_id: str, host: str, http_header: dict) -> dict:
    host = _validate_host(host)

    req = requests.post(
        f"{host}/api/2.0/clusters/start",
        headers=http_header,
        json={"cluster_id": cluster_id},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def update_cluster(
    cluster: dict,
    use_case: str,
    cluster_size: str,
    serverless: bool,
    run: int,
    host: str,
    http_header: dict,
):
    host = _validate_host(host)

    component = "job_cluster"
    benchmark_config = (
        "dbt__"
        + component
        + "__"
        + cluster_size.lower()
        + ("__serverless" if serverless else "__server-based")
    )

    # Build the custom tags
    custom_tags = cluster.get("custom_tags") if cluster.get("custom_tags") else {}
    custom_tags_copy = dict(custom_tags)
    custom_tags.update(
        {
            "benchmark_config_run": benchmark_config + "__run_" + str(run),
            "benchmark_config": benchmark_config,
            "benchmark_run_nr": str(run),
            "benchmark_use_case": use_case,
            "benchmark_component": component,
        }
    )

    if custom_tags == custom_tags_copy:
        print("    DBT: Tags already set. No need to update.")
        return

    # Need to supply the entire cluster to make sure it is not overwritten
    body = {
        "cluster_id": cluster.get("cluster_id"),
        "cluster_name": cluster.get("cluster_name"),
        "spark_version": cluster.get("spark_version"),
        "node_type_id": cluster.get("node_type_id"),
        "autotermination_minutes": cluster.get("autotermination_minutes"),
        "enable_elastic_disk": cluster.get("enable_elastic_disk"),
        "single_user_name": cluster.get("single_user_name"),
        "data_security_mode": cluster.get("data_security_mode"),
        "runtime_engine": cluster.get("runtime_engine"),
        "num_workers": cluster.get("num_workers"),
        "spark_env_vars": cluster.get("spark_env_vars"),
        "custom_tags": custom_tags,
    }

    req = requests.post(f"{host}/api/2.0/clusters/edit", headers=http_header, json=body)

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
