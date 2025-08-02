# Add packages to the Python path to be able to import them.
import argparse
import json
import os
import time
from datetime import datetime

from dbt.cli.main import dbtRunner, dbtRunnerResult

from edp_dbt_runner.helpers.databricks.auth import get_dbx_http_header, get_token
from edp_dbt_runner.helpers.databricks.utils import get_workspace_url
from edp_dbt_runner.helpers.databricks.workspace import workspace_import, workspace_mkdirs
from edp_dbt_runner.helpers.elementary import (
    commit_elementary_report_to_github,
    generate_elementary_report,
    run_edr_monitor,
)
from edp_dbt_runner.helpers.logger import get_logger, set_handler
from edp_dbt_runner.helpers.utils import (
    copy_dbt_project,
    create_zip_file,
    get_redacted_copy_of_dict,
)

# Importing dbt causes the following warning to appear over a 100 times in Python 3.10
# and higher (which is used in DBX 13.0 and up): "<frozen importlib._bootstrap>:914:
# ImportWarning: ImportHookFinder.find_spec() not found; falling back to find_module()"
# The workaround below is to prevent this (unnecessary) warning, to pop up and clog
# the logs.
# For more information: https://github.com/dbt-labs/dbt-core/issues/7828

# Create "root" logger and set handler
logger = get_logger("edp_dbt_runner", "INFO")
set_handler(logger)


def _get_default_vars() -> dict:
    """
    Get the default values for the vars that are needed as input to the project. These
    can be overrule by the user.
    """

    # A run from the workflow should not use prefixes by default.
    default_vars = {
        "prefix_schema": False,
        "prefix_table": False,
    }

    external_location_url = os.getenv("EXTERNAL_LOCATION_URL")

    if external_location_url != "n/a":
        default_vars["schema_location"] = external_location_url

    return default_vars


def _get_dbt_command(dbt_action, dbt_action_vars, default_dbt_vars) -> str:
    """
    Merge the task specific dbt_action_vars with the vars set by default into a single
    vars string and combine it with the dbt_command.
    """

    dbt_vars = json.dumps(default_dbt_vars | dbt_action_vars, separators=(",", ":"))

    return " ".join([dbt_action, "--vars", dbt_vars])


def run_dbt(
    dbt_command: str,
    environment_variables: dict,
):
    """
    This function will execute the provided dbt commands
    """

    log_safe_environment_variables = get_redacted_copy_of_dict(
        environment_variables, ["DBX_TOKEN"]
    )
    logger.info(
        f"Provided parameters: environment_variables: '{log_safe_environment_variables}'"
        f", dbt_command: '{dbt_command}'."
    )

    # Loop over all commands and execute
    logger.info("Starting dbt execution.")

    try:
        # Initialize
        dbt = dbtRunner()

        # Set environment variables
        for k, v in environment_variables.items():
            os.environ[k] = v

        # DBT commands need to be split in separate parameters
        cli_args = dbt_command.split(" ")
        logger.info(f"Executing commands {cli_args}")

        # Run the command
        res: dbtRunnerResult = dbt.invoke(cli_args)

        # Check the result
        if res.exception:
            raise res.exception
        elif not res.success:
            raise Exception(
                f"The DBT command '{dbt_command}' was unsuccessful. Please"
                " check the logs for more details."
            )
        elif hasattr(res.result, "__iter__"):
            # inspect the results
            error_nodes = [r for r in res.result if r.status not in ["success", "pass"]]
            if len(error_nodes) > 0:
                raise Exception(
                    f"{len(error_nodes)} object(s) failed to execute successfully. Please"
                    " check the logs for more details."
                )

    except Exception as ex:
        logger.error(ex)
        raise

    logger.info("Finished execution.")


def run():
    """
    This function is the only entry point to this package. It is designed
    to be used from inside a Databricks Workflow and therefore expects
    named commandline arguments as its input. This function will parse these
    arguments before starting dbt.
    """

    # Defining and fetching all arguments. All are required.
    parser = argparse.ArgumentParser(description="EDP DBT runner")
    parser.add_argument("--dbt_command", help="DBT command including flags except vars")
    parser.add_argument("--dbt_vars", help="Set all dbt vars")
    parser.add_argument("--environment_variables", help="JSON string with all env vars")
    parser.add_argument("--log_level", help="Python log level (DEBUG, INFO, etc)")
    parser.add_argument(
        "--context",
        help="JSON string with additional info for governance processes",
    )
    parser.add_argument("--elementary", help="JSON string with Elementary config")
    parser.add_argument("--continuous_config", help="JSON string with streaming config")
    parser.add_argument("--spark_env_vars", help="JSON string with spark_env_vars config")
    args = parser.parse_args()

    dbt_command = args.dbt_command
    dbt_vars = json.loads(args.dbt_vars)
    environment_variables = json.loads(args.environment_variables)
    log_level = args.log_level
    context = json.loads(args.context)
    elementary = json.loads(args.elementary)
    continuous_config = json.loads(args.continuous_config)
    spark_env_vars = json.loads(args.spark_env_vars)

    # Set the log level to whatever level provided.
    if log_level:
        logger.setLevel(log_level)

    logger.debug(f"Running with these arguments: {vars(args)}.")

    # Setting up the environment before we can run.
    # Copy the dbt project to the working folder.
    dbt_path = copy_dbt_project()
    # Get the hostname of the databricks workspace
    host = get_workspace_url()
    # Get a Databricks PAT for the current logged in user.
    token = get_token()

    # We provide this input as environment variables. We add them to
    # the once provided at runtime.
    default_environment_variables = {
        "DBT_PROJECT_DIR": dbt_path,
        "DBT_PROFILES_DIR": dbt_path,
        "DBX_HOST": host,
        "DBX_TOKEN": token,
    }
    environment_variables.update(default_environment_variables)
    environment_variables.update(spark_env_vars)  # Add spark_env_vars to env variables
    default_dbt_vars = _get_default_vars()

    if not any(i in dbt_command for i in ["docs", "source"]):
        dbt_command = _get_dbt_command(dbt_command, dbt_vars, default_dbt_vars)

    # Initialize variable to store possible exceptions during the run
    exceptions = []
    # Now run dbt. This can be continuous or a one time run
    try:

        if continuous_config.get("continuous") is True:
            # Fetch any limitations and run in a loop. A limitation can be set on the
            # total time it is running (in seconds) or the total amount of runs. Or both.
            # Setting any of these, makes it possible to finish a task without cancelling
            # it and causing a "failure". When combined with a continuous job, it can
            # create a continuous run of succesfull tasks.
            # Note: Both these settings must be set. If you don't want to apply the
            # limitation it can be set to 0.
            finish_after_seconds = continuous_config.get("finish_after_seconds")
            if not (finish_after_seconds):
                raise Exception(
                    "When the task has the 'continuous' property set, the "
                    "'finish_after_seconds' property must also be specified. Set to '0' "
                    "to keep the task running."
                )
            finish_after_runs = continuous_config.get("finish_after_runs")
            if not (finish_after_runs):
                raise Exception(
                    "When the task has the 'continuous' property set, the "
                    "'finish_after_runs' property must also be specified. Set to '0' to "
                    "keep the task running."
                )

            # Initialize counters and timer
            run_counter = 0
            start_time = datetime.now()
            seconds_running = (datetime.now() - start_time).total_seconds()

            while (finish_after_runs == 0 or run_counter < finish_after_runs) and (
                finish_after_seconds == 0 or (seconds_running < finish_after_seconds)
            ):
                run_dbt(dbt_command, environment_variables)

                # Update counters and timers
                run_counter += 1
                seconds_running = (datetime.now() - start_time).total_seconds()
                logger.debug(
                    f"Finished run {run_counter}, "
                    f"running for {seconds_running:,.0f} seconds now."
                )
        else:
            # If "continuous" is not set, simply run the command once
            run_dbt(dbt_command, environment_variables)
    except Exception as ex:
        exceptions.append(ex)

    # The run has finished but any error are ignored for now. If it happens we want to
    # upload the target folder to the workspace so it can be used for debugging. This
    # should also happen if the log level wat set to debug.
    if log_level == "DEBUG" or exceptions:
        http_header = get_dbx_http_header(token)

        try:
            # Check if the target folder was created
            dbt_target_folder = dbt_path + "/target"
            if os.path.exists(dbt_target_folder):
                # Create job/task folder
                target_path = (
                    "/Shared/edp_dbt_workflow_log/"
                    f"{context.get('job_name')}__{context.get('task_name')}"
                    f"__{time.strftime('%Y%m%d_%H%M%S')}"
                ).replace(" ", "_")
                workspace_mkdirs(target_path, host, http_header)

                # add the logs and the target folder to it
                workspace_import(
                    dbt_path + "/logs/dbt.log", target_path, False, host, http_header
                )
                # The target folder needs to be zipped first
                zip_file = create_zip_file(dbt_target_folder)
                workspace_import(zip_file, target_path, False, host, http_header)
                logger.info(f"DBT target folder is persisted at: {target_path}")
            else:
                logger.info(
                    f"DBT target folder ('{dbt_target_folder}') does not exists and can"
                    " therefor not be uploaded to the workspace."
                )

        except Exception as ex:
            logger.error(ex)
            exceptions.append(ex)

    if elementary.get("update_github_pages"):
        logger.info("Generating Elementary report to update GitHub pages.")

        try:
            edr_target_folder = dbt_path + "/edr_target"
            if "githubapp_id" not in context or context.get("githubapp_id") == "":
                raise Exception(
                    "Missing key 'githubapp_id' in context parameter. This key is"
                    " required to create an Elementary report."
                )

            generate_elementary_report(dbt_path, edr_target_folder, context, elementary)
            commit_elementary_report_to_github(edr_target_folder, context)
        except Exception as ex:
            logger.error(ex)
            logger.error(
                "Please refer to the 'DBT reference pipeline' documentation on how to"
                " setup Elementary reporting from a Databricks Workflow."
            )
            exceptions.append(ex)

    if elementary.get("teams_notification"):
        logger.info("Starting Elementary notification report.")
        try:
            run_edr_monitor()
        except Exception as ex:
            logger.error(ex)
            exceptions.append(ex)

    # At this point 0, 1 or 2 exceptions have occured. All error messages will be printed
    # through logging.error messages. Here we only raise the first exception (if any).
    # As soon as the databricks runtime uses python 3.11, it is also possible to use
    # an ExceptionGroup to raise them all.
    if exceptions:
        raise exceptions.pop(0)


# Fallback to check if this package is run without the proper entry point
if __name__ == "__main__":
    raise Exception("This package should be run by using the 'run' entry point.")
