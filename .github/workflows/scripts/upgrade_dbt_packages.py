import json
import os

from yaml import safe_dump, safe_load

DBT_PACKAGES_DIR = "dbt_transform"
DBT_PACKAGES_FILE = "packages.yml"
DBT_PACKAGES_UPGRADE_MANIFEST = "dbt_packages_upgrade_manifest.tmp"
DBT_NEWER_PACKAGES_VERSIONS_LOG_FILE = "newer_dbt_packages_versions.tmp"


def get_dbt_packages_file(package_file_path: str) -> dict:
    """
    Get the dbt packages from the packages.yml file and return it as a dictionary.
    """
    try:
        with open(package_file_path, "r") as file:
            dbt_packages = safe_load(file)
            file.close()
            if (
                dbt_packages is None
                or not dbt_packages["packages"]
                or not isinstance(dbt_packages, dict)
                and not len(dbt_packages.items()) > 0
            ):
                raise ValueError("No dbt packages found in the packages.yml file.")
    except Exception as e:
        raise e
    return dbt_packages


def write_file(file_path: str, content) -> None:
    """
    Write the content to the file with the given file path.
    """
    try:
        with open(file_path, "w") as file:
            file.write(safe_dump(content))
            file.close()
    except Exception as e:
        raise e


def get_abs_file_path(file_path: str) -> str:
    """
    Get the absolute file path from the given file path.
    """
    workspace = os.getenv("GITHUB_WORKSPACE")
    if workspace:
        return f"{workspace}/{file_path}"
    else:
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", file_path)
        )


def upgrade_dbt_packages_file(newer_versions_pkgs: dict) -> None:
    """
    Upgrade the dbt packages file with the newer versions provided,
    and write the changes to the manifest file,
    print the updated dbt packages and
    return True if the packages were upgraded, False otherwise.
    """
    file_path = f"{DBT_PACKAGES_DIR}/{DBT_PACKAGES_FILE}"
    abs_file_path = get_abs_file_path(file_path)
    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(f"File {abs_file_path} not found!")

    dbt_packages = get_dbt_packages_file(abs_file_path)

    is_all_dbt_pkgs_exist_in_newer_versions = all(
        pkg["package"] in newer_versions_pkgs for pkg in dbt_packages["packages"]
    )
    if not is_all_dbt_pkgs_exist_in_newer_versions:
        raise ValueError(
            "Not all dbt packages exist in the newer versions packages provided. \
            Please check the packages.yml file."
        )
    old_packages = {pkg["package"]: pkg["version"] for pkg in dbt_packages["packages"]}
    new_packages = {pkg: newer_versions_pkgs[pkg] for pkg in newer_versions_pkgs}

    upgraded = False
    for i in dbt_packages["packages"]:
        pkg = i["package"]
        if pkg in newer_versions_pkgs:
            if i["version"] != newer_versions_pkgs[pkg]:
                upgraded = True
            i["version"] = newer_versions_pkgs[pkg]

    print("Updated dbt packages: ", str(json.dumps(dbt_packages, indent=2)))

    if upgraded:
        write_file(abs_file_path, dbt_packages)
        abs_file_path_manifest = abs_file_path.replace(
            DBT_PACKAGES_FILE, DBT_PACKAGES_UPGRADE_MANIFEST
        )
        content = {"Current versions": old_packages, "Changed versions": new_packages}
        write_file(abs_file_path_manifest, content)

    return upgraded


def get_val_from_split_list(split_list, index):
    """
    Get the value from the split list at the given index.
    """
    return split_list[index].strip() if len(split_list) > index else None


def stack_new_val_to_dict_first_tuple(dict: dict, value):
    """
    Stack the new value to the first tuple of the dictionary.
    """
    item = dict.popitem()
    dict[item[0]] = value


def get_newer_dbt_packages_versions() -> dict:
    """
    Get the newer dbt packages versions from the logs and return it as a dictionary.
    """
    file_path = f"{DBT_PACKAGES_DIR}/{DBT_NEWER_PACKAGES_VERSIONS_LOG_FILE}"
    abs_file_path = get_abs_file_path(file_path)
    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(f"File {abs_file_path} not found!")
    try:
        with open(abs_file_path, "r") as file:
            logs = file.readlines()
            for i in range(len(logs)):
                logs[i] = logs[i].split("\n")
            file.close()
    except Exception as e:
        raise e

    packages_from_logs = {}
    for line in logs:
        for seq in line:
            installing_split = seq.split("Installing")
            package = get_val_from_split_list(installing_split, 1)
            if package:
                packages_from_logs[package] = ""

            version_split = seq.split("Installed from version")
            version = get_val_from_split_list(version_split, 1)
            if version:
                stack_new_val_to_dict_first_tuple(packages_from_logs, version)

            available_versions_split = seq.split("Updated version available:")
            available_version = get_val_from_split_list(available_versions_split, 1)
            if available_version:
                stack_new_val_to_dict_first_tuple(packages_from_logs, available_version)

    return packages_from_logs


def main() -> None:
    dbt_newer_versions_log = get_newer_dbt_packages_versions()
    upgrade_dbt_packages_file(dbt_newer_versions_log)


if __name__ == "__main__":
    main()
