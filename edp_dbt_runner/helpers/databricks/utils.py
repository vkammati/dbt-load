from pyspark.sql import SparkSession

from edp_dbt_runner.helpers.logger import get_logger

logger = get_logger(__name__)


def validate_host(host: str) -> str:
    """
    Validate a Databricks host url and correct if needed. A valid url for use in api
     calls will be returned.
    :param host: Databricks host
    """
    # Host must begin with https://
    if host[:8] != "https://":
        host = f"https://{host}"

    # Host must NOT end with a /
    if host[-1:] == "/":
        host = host[:-1]
    return host


def get_workspace_url() -> str:
    """
    This function will return the workspace url, also known as host(name)
    (e.g. adb-123456789.0.azuredatabricks.net) of the current databricks
    workspace.
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        if workspace_url:
            return workspace_url
        raise Exception("spark config 'spark.databricks.workspaceUrl' is empty")

    except Exception as ex:
        logger.error(ex)
        raise
