import os
from shutil import copytree, make_archive
from uuid import uuid4

from edp_dbt_runner.helpers.logger import get_logger

logger = get_logger(__name__)


def get_package_path() -> str:
    """
    This function will return the local path where this 'edp_dbt_runner'
    package is installed.
    """

    import edp_dbt_runner as _

    package_path = _.__path__

    logger.debug(f"edp_dbt_runner.dbt_runner.__path__: {str(package_path)}")

    if package_path:
        return str(package_path[0])
    else:
        return ""


def copy_dbt_project() -> str:
    """
    This function will copy the dbt project from of the package folder and
    into a unique folder inside the current working folder. This is required
    when running this package on a 'shared' cluster as these do not have
    permissions to write (packages, logs, etc) into the package folder.
    """

    src_path = get_package_path() + "/dbt"
    logger.debug(f"Source path: {src_path}")

    current_folder = os.getcwd()
    unique_id = str(uuid4())
    dst_path = current_folder + "/dbt/" + unique_id
    logger.debug(f"Destination path: {dst_path}")

    copytree(src_path, dst_path)

    return dst_path


def get_redacted_copy_of_dict(
    dict_to_clean: dict, keys_to_redact: list[str], replace_with: str = "[REDACTED]"
) -> dict:
    """
    This function can be used to redact a dictionary so that it can safely be
    logged. All keys provided will be replaced by an optionally provided string.
    The function will not alter the passed in dictionary itself.
    """
    # Create shallow copy of the dictionary to make sure the original dictionary is
    # not altered
    copy_of_dict = dict(dict_to_clean)

    # Loop over the provided list of keys and replace all of the
    for k in keys_to_redact:
        copy_of_dict[k] = replace_with

    return copy_of_dict


def create_zip_file(path: str):
    """
    This function can be used to create a zip file containing all the files and folders
    in the provided 'path'. The resulting filename will be named after folder name.
    This function will return the entire path to the created zip file.
    """
    # When used for uploading to Databricks, the structure inside the zip file must start
    # with 1 main folder. All subfolders and files must be within that main folder.
    # we set the root_dir and base_dir to achieve this.
    zip_file = make_archive(
        path, "zip", root_dir=os.path.dirname(path), base_dir=os.path.basename(path)
    )
    return zip_file


def get_secret_from_scope(databricks_scope: str, secret_key: str) -> str:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    # instantiate common variables; needed when loading packages from library
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    return dbutils.secrets.get(scope=databricks_scope, key=secret_key)
