import datetime
import os

from helpers.databricks.workspace import workspace_list
from packaging.version import Version, parse


def get_workspace_wheel_path(wheel_name: str) -> str:
    return f"/Shared/{wheel_name}"


def get_version_from_wheel_path(file_path: str) -> Version:
    # strip the path to leave only the filename
    file_name = os.path.basename(file_path)

    # version should be the fourth element from the end. This way,package
    # name can still contain a -
    version = file_name.split("-")[-4]

    # Convert to Version object for comparison operations
    return parse(version)


def get_wheels_from_workspace(path: str, host: str, http_header: dict) -> list[dict]:
    # Get all files in the provided workspace path
    folder_content = workspace_list(path=path, host=host, http_header=http_header)

    # If folder is not empty, filter out all the wheels and add the version to the dict
    list_of_wheels = []
    if folder_content:
        list_of_wheels = [
            dict(f, **{"version": get_version_from_wheel_path(f["path"])})
            for f in folder_content
            if f["object_type"] == "FILE" and f["path"][-3:] == "whl"
        ]

    return list_of_wheels


def get_wheels_to_delete(
    list_of_wheels: list[dict], wheels_to_keep: int, nr_of_days_to_keep_wheel: int
) -> list[dict]:
    # build list of wheels to delete
    wheels_to_delete = []

    if len(list_of_wheels) > 0:
        cut_of_date = datetime.datetime.today() - datetime.timedelta(
            days=nr_of_days_to_keep_wheel
        )

        print(f"The most recent {wheels_to_keep} wheel(s) will not be deleted.")
        print(
            f"Wheels that were created on {cut_of_date.strftime('%Y-%m-%d')}"
            " or later will not be deleted."
        )

        # Loop over wheels (sorted on created_at) except the last nr of wheels to keep
        for wheel in sorted(list_of_wheels, key=lambda x: x["created_at"])[
            :-wheels_to_keep
        ]:
            wheel_modified_at = datetime.datetime.fromtimestamp(wheel["created_at"] / 1e3)
            if wheel_modified_at < cut_of_date:
                wheels_to_delete.append(wheel)

    return wheels_to_delete
