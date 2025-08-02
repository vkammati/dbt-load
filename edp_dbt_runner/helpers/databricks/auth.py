import logging

from databricks.sdk import WorkspaceClient

from edp_dbt_runner.helpers.databricks.logger import set_databricks_sdk_logger
from edp_dbt_runner.helpers.logger import get_logger

logger = get_logger(__name__)


def get_token(lifetime_in_seconds: int = 10800) -> str:
    """
    This function will use the databricks sdk package to get a
    databricks PAT for the current user. This will be the 'run as' user
    when ran from a Databricks Workflow.
    """
    # Initialize sdk logger
    set_databricks_sdk_logger()

    # Create a Workspace Client, optionally setting the debug headers.
    w = WorkspaceClient(debug_headers=logger.getEffectiveLevel == logging.DEBUG)

    # Generate token
    logger.debug(f"Generating token for '{w.current_user.me().display_name}'.")
    token = w.tokens.create(lifetime_seconds=lifetime_in_seconds)

    return token.token_value


# This function generates the HTTP header for the Databricks REST API.
def get_dbx_http_header(token: str) -> dict:
    """Generate the HTTP header for the Databricks REST API.

    :param token: oAuth token or PAT (Personal Access Token)
    :return: the HTTP header
    """
    return {
        "Authorization": f"Bearer {token}",
    }
