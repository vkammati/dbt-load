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


def get_sql_warehouse(warehouse_id: int, host: str, http_header: dict) -> dict:
    host = _validate_host(host)
    req = requests.get(
        f"{host}/api/2.0/sql/warehouses/{warehouse_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def update_sql_warehouse(
    warehouse_id: int,
    use_case: str,
    cluster_size: str,
    serverless: bool,
    run: int,
    host: str,
    http_header: dict,
):

    host = _validate_host(host)

    component = "sql"
    benchmark_config = (
        "dbt__"
        + component
        + "__"
        + cluster_size.lower()
        + ("__serverless" if serverless else "__server-based")
    )

    req = requests.post(
        f"{host}/api/2.0/sql/warehouses/{warehouse_id}/edit",
        headers=http_header,
        json={
            "cluster_size": cluster_size,
            "enable_serverless_compute": serverless,
            "warehouse_type": "PRO" if serverless else "CLASSIC",
            "tags": {
                "custom_tags": [
                    {
                        "key": "benchmark_config_run",
                        "value": benchmark_config + "__run_" + str(run),
                    },
                    {"key": "benchmark_config", "value": benchmark_config},
                    {"key": "benchmark_run_nr", "value": str(run)},
                    {"key": "benchmark_use_case", "value": use_case},
                    {"key": "benchmark_component", "value": component},
                ]
            },
        },
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def start_sql_warehouse(warehouse_id: int, host: str, http_header: dict):

    host = _validate_host(host)
    req = requests.post(
        f"{host}/api/2.0/sql/warehouses/{warehouse_id}/start",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
