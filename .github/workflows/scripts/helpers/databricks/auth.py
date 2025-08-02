from helpers.aws.token import get_aws_access_token
from helpers.azure.token import get_azure_access_token
from helpers.databricks.utils import get_cloud_platform_from_host


# This function gets an access token that can be used to authenticate to the Databricks
# REST API. The host determines whether to authenticate to azure or to aws.
def get_access_token(host: str) -> str:
    cloud_platform = get_cloud_platform_from_host(host)
    if cloud_platform == "azure":
        return get_azure_access_token()
    elif cloud_platform == "aws":
        return get_aws_access_token()
    else:
        raise ValueError(f"Cloud platform '{cloud_platform}' is not supported.")


# This function generates the HTTP header for the Databricks REST API.
def get_dbx_http_header(token: str) -> dict:
    """Generate the HTTP header for the Databricks REST API.

    :param token: oAuth token or PAT (Personal Access Token)
    :return: the HTTP header
    """
    return {
        "Authorization": f"Bearer {token}",
    }
