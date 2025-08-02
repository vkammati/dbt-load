import base64
import os

from azure.identity import CertificateCredential, ClientSecretCredential


def get_azure_access_token() -> str:
    # The DefaultCredential flow expects the AZURE_CLIENT_CERTIFICATE_PATH to be set
    # when using the certificate. The workaround below makes it possible to set the
    # content of the certificate as base64 string in 'AZURE_CLIENT_CERTIFICATE' which
    # will then be used in conjunction with other environment variables. This is easier
    # to use from github workflow and prevents having to commit the certificate to disk.
    if str(os.getenv("AZURE_CLIENT_CERTIFICATE") or "").strip() != "":
        # Encode string into bytes
        certificate_data = base64.b64decode(
            os.getenv("AZURE_CLIENT_CERTIFICATE").encode("ascii")
        )
        # Create the credentials using the 'other' environment variables. the password
        # is stripped and encoded. This makes it possible to work with certificates that
        # are not password protected by simply not adding the password GitHub secret or
        # leaving it an empty string.
        credential = CertificateCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("CLIENT_ID"),
            certificate_data=certificate_data,
            password=os.getenv("AZURE_CLIENT_CERTIFICATE_PASSWORD")
            .strip()
            .encode("utf-8"),
        )
    else:
        # If there is no client certificate set, use the client secret flow
        azure_tenant_id = os.getenv("AZURE_TENANT_ID")
        azure_client_id = os.getenv("CLIENT_ID")
        azure_client_secret = os.getenv("CLIENT_SECRET")

        credential = ClientSecretCredential(
            tenant_id=azure_tenant_id,
            client_id=azure_client_id,
            client_secret=azure_client_secret,
        )

    # Now fetch a token and return it.
    try:
        # default scope is set to Databricks
        scope = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
        ad_token = credential.get_token(scope)
        return ad_token.token
    except Exception:
        raise
