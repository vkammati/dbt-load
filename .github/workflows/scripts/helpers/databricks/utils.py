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


def get_cloud_platform_from_host(host: str) -> str:
    """
    Get the cloud platform based on the host url.
    """
    if "azuredatabricks" in host.lower():
        return "azure"
    else:
        return "aws"
