import requests
from requests.exceptions import HTTPError

from edp_dbt_runner.helpers.logger import get_logger

logger = get_logger(__name__)

_github_api_url = "https://api.github.com"


def get_app_installation(org: str, repo: str, http_header: dict) -> list[dict]:
    """
    Get the installation of the app that corresponds to the provided token in the
    given org and repo.

    :param org: GitHub organization the GitHub app is installed in
    :param repo: GitHub repon the GitHub app is installed in
    :param http_header: HTTP header used for the GitHub REST API
    """
    logger.debug(f"Getting GitHub app installation for '{org}/{repo}'.")

    req = requests.get(
        f"{_github_api_url}/repos/{org}/{repo}/installation",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        logger.error(e.response.text)
        raise e

    return req.json()


def get_app_access_token(installation_id: int, http_header: dict):
    """
    Get a broad access_token for the given github app

    :param installation_id: Installation id of GitHub App
    :param http_header: HTTP header used for the GitHub REST API
    """

    logger.debug(
        f"Getting GitHub app access_token for installation id '{installation_id}'."
    )

    # This will get an access_token wiht that has all permissions given to the app and
    # for all repo's it has been installed in. Request can be limited if needed.
    req = requests.post(
        f"{_github_api_url}/app/installations/{installation_id}/access_tokens",
        headers=http_header,
    )

    try:
        req.raise_for_status()
        return req.json()
    except HTTPError as e:
        logger.error(e.response.text)
        raise e
