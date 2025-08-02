import requests
from requests.exceptions import HTTPError


def generate_spn_ad_token(
    tenant_id: str, spn_client_id: str, spn_client_secret: str, scope: str
) -> str:
    """Generate a short lived AD token for a specific SPN.

    :param tenant_id: the Azure tenant id
    :param spn_client_id: the service principal client id
    :param spn_client_secret: the service principal client secret
    :return: the generated AD token for the service principal
    """
    req = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_id": spn_client_id,
            "grant_type": "client_credentials",
            "scope": scope,
            "client_secret": spn_client_secret,
        },
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["access_token"]
