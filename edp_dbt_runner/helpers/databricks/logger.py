from edp_dbt_runner.helpers.logger import get_logger, set_handler

logger = get_logger(__name__)


def set_databricks_sdk_logger():
    """
    This helper function will get the logger of the databricks.sdk package
    and align it with the current package logger.
    """
    # Get the databricks sdk logger and set it to the same level as our root logger
    # and set the default handler to add its logs to our own.
    databricks_sdk_logger = get_logger("databricks.sdk", logger.getEffectiveLevel())
    set_handler(databricks_sdk_logger)
