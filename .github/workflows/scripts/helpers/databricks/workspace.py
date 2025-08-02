import os
from base64 import b64encode

import requests
from helpers.databricks.utils import validate_host
from requests.exceptions import HTTPError


def workspace_mkdirs(path: str, host: str, http_header: dict):
    """Create folder in Databricks workspace if it doesn't exist.
    :param path: folder path to create in Workspace (also works for Repos)
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    host = validate_host(host)
    req = requests.post(
        f"{host}/api/2.0/workspace/mkdirs",
        headers=http_header,
        json={
            "path": path,
        },
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def workspace_list(path: str, host: str, http_header: dict) -> list[dict]:
    """Get the content of a folder

    :param path: Workspace's folder path
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    host = validate_host(host)
    req = requests.get(
        f"{host}/api/2.0/workspace/list",
        headers=http_header,
        json={"path": path},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json().get("objects")


def workspace_delete(path: str, host: str, http_header: dict):
    """Delete the provided path

    :param path: Path to object to delete
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    host = validate_host(host)
    req = requests.post(
        f"{host}/api/2.0/workspace/delete",
        headers=http_header,
        json={"path": path},
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def workspace_import(
    source_path: str, path: str, overwite: bool, host: str, http_header: dict
):
    """Import the provided source_path into path

    :param source_path: Path to local file to import
    :param path: Path to the Databricks folder to import the file into
    :param overwrite: indicate whether the file should be overwritten if
        it already exists.
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    host = validate_host(host)
    file_name = os.path.basename(source_path)
    path = f"{path}/{file_name}"

    with open(source_path, "rb") as f:
        # import_workspace must take content that is typed str.
        content = b64encode(f.read()).decode()

        req = requests.post(
            f"{host}/api/2.0/workspace/import",
            headers=http_header,
            json={
                "path": path,
                "format": "AUTO",
                "content": content,
                "overwrite": overwite,
            },
        )

        try:
            req.raise_for_status()
        except HTTPError as e:
            print(e.response.text)
            raise e
