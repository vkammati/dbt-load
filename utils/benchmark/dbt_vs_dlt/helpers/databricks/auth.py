def get_dbx_http_header(pat: str) -> dict:
    """Generate the HTTP header for the Databricks REST API.

    :param pat: Databricks Personal Access Token
    :return: the HTTP header
    """
    return {
        "Authorization": f"Bearer {pat}",
    }
