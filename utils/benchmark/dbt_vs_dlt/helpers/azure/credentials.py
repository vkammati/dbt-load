import os
from argparse import ArgumentParser, Namespace

from helpers.azure.token import generate_spn_ad_token

# default scope is set to Databricks
_default_scope = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"


def add_spn_arguments(parser: ArgumentParser):
    parser.add_argument("--azure-tenant-id", help="Azure tenant id.", type=str)
    parser.add_argument(
        "--azure-client-id",
        help="Service principal client id used to deploy code.",
        type=str,
    )
    parser.add_argument(
        "--azure-client-secret",
        help="Service principal client secret used to deploy code.",
        type=str,
    )


def get_spn_credentials(args: Namespace) -> tuple[str, str, str]:
    if args.azure_client_id:
        # Using credentials provided through arguments
        azure_tenant_id = args.azure_tenant_id
        azure_client_id = args.azure_client_id
        azure_client_secret = args.azure_client_secret
    else:
        # Using credentials provided in environment variables.

        azure_tenant_id = os.getenv("AZURE_TENANT_ID")
        azure_client_id = os.getenv("AZURE_CLIENT_ID")
        azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")

        if not azure_tenant_id:
            raise Exception("Missing envrionment variable 'AZURE_TENANT_ID'.")
        if not azure_client_id:
            raise Exception("Missing envrionment variable 'AZURE_CLIENT_ID'.")
        if not azure_client_secret:
            raise Exception("Missing envrionment variable 'AZURE_CLIENT_SECRET'.")

    return azure_tenant_id, azure_client_id, azure_client_secret


def get_ad_token(args: Namespace, scope: str = None) -> str:
    # Get the needed credentials
    azure_tenant_id, azure_client_id, azure_client_secret = get_spn_credentials(args)

    # If scope is not supplied, use the default scope
    if not scope:
        scope = _default_scope

    # Get AD token and return it.
    try:
        return generate_spn_ad_token(
            tenant_id=azure_tenant_id,
            spn_client_id=azure_client_id,
            spn_client_secret=azure_client_secret,
            scope=scope,
        )
    except Exception:
        raise
