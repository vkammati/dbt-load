"""
This script is used to get (output is printed) the latest deployed version of a wheel
"""

import os

from helpers.databricks.auth import get_access_token

if __name__ == "__main__":
    # determine host
    host = os.getenv("DATABRICKS_HOST")

    # Get access token token and use it to build the api header. The host determines
    # whether to authenticate to azure or to aws.
    access_token = get_access_token(host=host)
    print(access_token)
