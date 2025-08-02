import os

from databricks.sdk import WorkspaceClient


def get_aws_access_token(lifetime_in_seconds: int = 600) -> str:
    host = os.getenv("DATABRICKS_HOST")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    w = WorkspaceClient(
        debug_headers=True,
        auth_type="oauth-m2m",
        host=host,
        client_id=client_id,
        client_secret=client_secret,
    )

    token = w.tokens.create(lifetime_seconds=lifetime_in_seconds)

    return token.token_value
