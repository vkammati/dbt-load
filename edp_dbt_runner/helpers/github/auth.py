import datetime

import jwt

from edp_dbt_runner.helpers.logger import get_logger
from edp_dbt_runner.helpers.utils import get_secret_from_scope

logger = get_logger(__name__)


def get_private_key(repo: str, environment: str) -> str:
    """
    Read the private-key file fro the package and return its content
    """
    try:
        databricks_scope = f"{repo}_{environment}_edp_dbt_runner"
        return get_secret_from_scope(databricks_scope, "GITHUBAPP-PRIVATE-KEY")

    except Exception:
        raise


def get_jwt_token(githubapp_id: int, private_key: str) -> str:
    """
    Generate a jwt token for use wiht a GitHub App. The token is valid for 10 minutes.
    """
    payload = {
        "iat": int(datetime.datetime.timestamp(datetime.datetime.now())),
        "exp": int(datetime.datetime.timestamp(datetime.datetime.now())) + (10 * 60),
        "iss": githubapp_id,
    }
    return jwt.encode(payload, private_key, algorithm="RS256")


# This function generates the HTTP header for the Databricks REST API.
def get_github_http_header(token: str) -> dict:
    """
    Generate the HTTP header for the GitHub REST API.

    :param token: JWT token or access token
    :return: the HTTP header
    """
    return {
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "accept": "application/vnd.github+json",
    }
