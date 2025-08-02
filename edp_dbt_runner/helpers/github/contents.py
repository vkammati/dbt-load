import os
from base64 import b64encode

import requests
from requests.exceptions import HTTPError

from edp_dbt_runner.helpers.logger import get_logger

logger = get_logger(__name__)

_github_api_url = "https://api.github.com"


def get_content(
    org: str,
    repo: str,
    branch: str,
    repo_path: str,
    http_header: dict,
):
    """
    Gets the contents of a file or directory in a GitHub repository. If the path or
    file cannot be found, an empty dictionary is returned.

    :param org: GitHub organization the repository is in
    :param repo: GitHub repo the file/directory is in
    :param branch: GitHub branch the file/directory is in
    :param repo_path: GitHub repo path to the directory or file.
    :param http_header: HTTP header used for the GitHub REST API
    """

    logger.debug(
        f"Checking existence of '{org}/{repo}/{repo_path}' in branch '{branch}'."
    )

    req = requests.get(
        f"{_github_api_url}/repos/{org}/{repo}/contents/{repo_path}?ref={branch}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
        return req.json()
    except HTTPError as e:
        if e.response.status_code == 404:
            logger.debug(f"Path '{e.response.url}' does not exists.")
            return {}
        logger.error(e.response.text)
        raise e


def create_or_update_file_content(
    org: str,
    repo: str,
    branch: str,
    repo_path: str,
    source_path: str,
    commit_message: str,
    sha: str,
    http_header: dict,
):
    """
    Create or update a file in GitHub

    :param org: GitHub organization the repository is in
    :param repo: GitHub repo file should created/updated in
    :param branch: GitHub branch file should created/updated in
    :param repo_path: GitHub repo path the file should be committed to without file name
    :param source_path: Full local path to the file that will be committed
    :param commit_message: GitHub message used for the commit
    :param sha: sha of the file that will be updated in case it already exists. None for
                a new file.
    :param http_header: HTTP header used for the GitHub REST API
    """

    body = {
        "branch": branch,
        "message": commit_message,
    }
    if sha:
        body["sha"] = sha

    file_name = os.path.basename(source_path)

    logger.debug(f"Committing '{file_name}' to '{repo_path}' in branch '{branch}'.")

    with open(source_path, "rb") as f:
        # import_workspace must take content that is typed str.
        content = b64encode(f.read()).decode()
        body["content"] = content

        req = requests.put(
            f"{_github_api_url}/repos/{org}/{repo}/contents/{repo_path}/{file_name}",
            headers=http_header,
            json=body,
        )

        try:
            req.raise_for_status()
            return req.json()
        except HTTPError as e:
            # logger.error(e.response.text)
            raise e
