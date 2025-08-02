from pathlib import Path

import yaml
from pydantic import ValidationError
from validation_schema.cluster_validation_schema import DbxClustersConfig
from validation_schema.job_validation_schema import DbxJobsConfig
from validation_schema.sql_wh_validation_schema import DbxSqlWarehousesConfig

file_name_to_schema = {
    "databricks_cluster.yml": DbxClustersConfig,
    "databricks_dbt_job.yml": DbxJobsConfig,
    "databricks_sql_warehouse.yml": DbxSqlWarehousesConfig,
}


def _load_yml(path: Path):
    """
    Ready yml
        Parameters:
            yml_path (str): path to yml file
        Returns:
            yml_dict (dict): dictionary with yml content
    """
    with open(path, "r", encoding="utf-8") as file:
        yml_dict = yaml.safe_load(file)
    return yml_dict


def _is_valid(file_name: str, yml_dict: dict) -> tuple[bool, str]:
    """
    Validate yml file
        Parameters:
            yml_path (str): path to yml file
        Returns:
            is_valid (bool): True if yml is valid, False otherwise
    """

    schema = file_name_to_schema.get(file_name)

    if schema is None:
        return True, "Warning - No schema found for this file"
    try:
        schema(**yml_dict)
        return True, "Valid"
    except ValidationError as exc:
        return False, str(exc)


def main():
    """
    Use pydantic to validate the yml files in ./config/**
    """

    # List of all yml files in the root and sub folders
    config_paths = Path("config").glob("**/*.yml")

    errors: list[str] = []

    for path in config_paths:
        yml_dict = _load_yml(path)

        is_valid, msg = _is_valid(path.name, yml_dict)
        if is_valid:
            print(f">>>{path}: {msg}")
        else:
            errors.append(f"\n\n >>>Error in {path}: {msg}")

    if errors:
        raise Exception("\n".join(errors))


if __name__ == "__main__":
    main()
