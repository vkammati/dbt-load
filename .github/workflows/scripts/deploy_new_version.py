"""
This script is used to deploy a new wheel to the workspace
"""

import argparse
import os

from helpers.actions import create_warning
from helpers.databricks.auth import get_access_token, get_dbx_http_header
from helpers.databricks.workspace import workspace_import, workspace_mkdirs
from helpers.wheel import (
    get_version_from_wheel_path,
    get_wheels_from_workspace,
    get_workspace_wheel_path,
)

parser = argparse.ArgumentParser(description="Deploy new version")

parser.add_argument("--wheel-name", help="Name of the wheel", type=str, required=True)
parser.add_argument(
    "--source",
    help="File path to the source file to be uploaded",
    type=str,
    required=True,
)

args = parser.parse_args()


def main():
    # determine host
    host = os.getenv("DATABRICKS_HOST")

    # Get access token token and use it to build the api header. The host determines
    # whether to authenticate to azure or to aws.
    access_token = get_access_token(host=host)
    http_header = get_dbx_http_header(access_token)

    # Get folder path for wheel. Create it if it does not exists.
    folder_path = get_workspace_wheel_path(args.wheel_name)
    workspace_mkdirs(path=folder_path, host=host, http_header=http_header)

    # Get version from source file
    source_version = get_version_from_wheel_path(args.source)
    print(f"Deploying wheel version: {source_version}.")

    # Check if wheel has already been uploaded
    list_of_wheels = get_wheels_from_workspace(
        path=folder_path, host=host, http_header=http_header
    )

    if list_of_wheels:
        if source_version in [wheel["version"] for wheel in list_of_wheels]:
            create_warning(
                title="Wheel version already deployed",
                message=f"Version '{source_version}' is already deployed and will"
                " NOT be overwritten.",
            )
            return

    # Deploy the new version
    print("Starting deploy.")
    workspace_import(
        source_path=args.source,
        path=folder_path,
        overwite=False,
        host=host,
        http_header=http_header,
    )
    print("Done.")


if __name__ == "__main__":
    main()
