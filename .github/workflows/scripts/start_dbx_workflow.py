"""
This script is used to deploy a new wheel to the workspace
"""

import argparse
import os

import yaml
from helpers.actions import create_warning
from helpers.databricks.auth import get_access_token, get_dbx_http_header
from helpers.databricks.jobs import get_job_id, start_job

parser = argparse.ArgumentParser(description="Start one or more databricks jobs")

parser.add_argument(
    "--workflow-names",
    nargs="*",
    type=str,
    help="Name of the databricks workflow(s) to start",
)
parser.add_argument(
    "--workflow-definition-yaml-path",
    type=str,
    help="Path to a yaml file that holds the workflow definitions.",
)

args = parser.parse_args()


def main():
    # determine host
    host = os.getenv("DATABRICKS_HOST")

    # Get access token token and use it to build the api header. The host determines
    # whether to authenticate to azure or to aws.
    access_token = get_access_token(host=host)
    http_header = get_dbx_http_header(access_token)

    # First, build a list of workflows to start. They can be supplied directly using the
    # workflow-names argument and/or by specifying the path to the job definition yaml.
    workflows = []

    # Because the 'nargs' is set on the workflow-names argument, this variable will be
    # a list of strings. If provided, add it to the workflow list, except empty strings
    if args.workflow_names:
        workflows += [w for w in args.workflow_names if w.strip() != ""]

    # The path to the job definition yaml used to created the jobs can (also) be
    # supplied.
    workflow_yaml_path = args.workflow_definition_yaml_path
    if workflow_yaml_path:
        with open(workflow_yaml_path, "rb") as yml_file:
            workflow_yaml = yaml.safe_load(yml_file)

        # Get all jobs marked with 'trigger_once_after_deploy' and add them to the list
        workflows += [
            w.get("name").strip()
            for w in workflow_yaml.get("jobs")
            if w.get("trigger_once_after_deploy") and w.get("name") not in workflows
        ]

    # Loop over the all the found workflows to start them one by one.
    started_workflows = 0
    for workflow_name in workflows:
        # First translate the job name into a job id
        job_id = get_job_id(workflow_name, host=host, http_header=http_header)

        # Start the job when found
        if job_id:
            print(f"Starting workflow '{workflow_name}' with id '{job_id}'.")
            start_job(job_id=job_id, queue=True, host=host, http_header=http_header)
            started_workflows += 1
            print("Workflow started.")
        else:
            create_warning(
                title="Workflow does not exists",
                message=f"Workflow '{workflow_name}' does not exist and is therefor not"
                " started.",
            )

    # Print the final outcome
    if started_workflows > 0:
        print(
            f"Done. {len(workflows)} workflow(s) supplied, {started_workflows} found and"
            " started."
        )
    elif workflows:
        print(
            f"Done. NONE of the provided workflow(s) ({workflows}) were found. Please"
            " check the name(s)."
        )
    else:
        print("Done. No workflows to start.")


if __name__ == "__main__":
    main()
