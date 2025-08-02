import os
from datetime import date, datetime
from subprocess import CalledProcessError, run

from edp_dbt_runner.helpers.github.app import get_app_access_token, get_app_installation
from edp_dbt_runner.helpers.github.auth import (
    get_github_http_header,
    get_jwt_token,
    get_private_key,
)
from edp_dbt_runner.helpers.github.contents import (
    create_or_update_file_content,
    get_content,
)
from edp_dbt_runner.helpers.logger import get_logger

logger = get_logger(__name__)

_github_page_branch = "github_pages"


def generate_elementary_report(
    dbt_path: str, report_path: str, context: dict, elementary: dict
):

    # Set the base command to run an elementary report with some default configurations
    edr_command = [
        "edr",
        "report",
        "--target-path",
        report_path,
        "--project-dir",
        dbt_path,
        "--profiles-dir",
        dbt_path,
        "--open-browser",
        "false",
        "--update-dbt-package",
        "false",
        "--project-name",
        context.get("github_repository"),
    ]
    # It will show up as the name of the environment in the Elementary UI.
    environment = context.get("environment")
    if environment == "prd":
        edr_command.extend(["--env", "prod"])
    elif environment == "tst":
        edr_command.extend(["--env", "test"])
    elif environment == "uat":
        edr_command.extend(["--env", "uat"])
    else:
        edr_command.extend(["--env", "development"])

    # By default the Elementary report will use the past 7 days as input. This can be
    # overruled by supplying a fixed start date and/or fixed number of days to consider.
    # If both are set, the lowest number of the two will be used.
    days_back = -1
    if elementary.get("earliest_date_back"):
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        days_back = (
            date.today()
            - datetime.strptime(elementary.get("earliest_date_back"), fmt).date()
        ).days
    if elementary.get("max_days_back"):
        days_back = (
            days_back
            if days_back > 0 and days_back < elementary.get("max_days_back")
            else elementary.get("max_days_back")
        )
    if days_back > 0:
        edr_command.extend(["--days-back", str(days_back)])
    logger.debug(f"Running: {' '.join(edr_command)}")

    # Now run the command and capture the output.
    try:
        run_result = run(
            edr_command,
            capture_output=True,
        )
        run_result.check_returncode()
    except CalledProcessError as ex:
        if ex.stderr != b"":
            logger.error(ex.stderr.decode("utf-8"))
        elif ex.stdout != b"":
            logger.error(ex.stdout.decode("utf-8"))
        raise

    logger.info("Elementary report successfully generated.")


def commit_elementary_report_to_github(report_path: str, context: dict):
    # Gather the needed information from the provided context dict
    githubapp_id = context.get("githubapp_id")
    github_organisation = context.get("github_organisation")
    github_repository = context.get("github_repository")
    environment = context.get("environment")

    # Generate a jwt token using the GitHub App id and its private key. The jwt token
    # is used as the header of all requests to the 'app' endpoint'
    private_key = get_private_key(github_repository, environment)
    jwt_token = get_jwt_token(githubapp_id, private_key)
    logger.debug("JWT token generated for GitHub App")
    app_http_header = get_github_http_header(jwt_token)

    # A GitHub App can be installed in multiple repo's. Get the installation id.
    app_installation = get_app_installation(
        github_organisation, github_repository, app_http_header
    )
    app_installation_id = app_installation.get("id")
    logger.debug(f"Installation id of GitHub App is: {app_installation_id}")

    # Get an access_token for the GitHub App that can be used with all the other GitHub
    # api's
    access_token = get_app_access_token(app_installation_id, app_http_header)
    logger.debug("Access token generated for GitHub App")

    # Wrap the new token into a new http header
    github_http_header = get_github_http_header(access_token.get("token"))

    # The GitHub api can be used to both create and replace files. If the file already
    # exists, the 'sha' of that file need to be supplied to be able to replace it. Here
    # we check if the file exists and, if so, capture its sha for use in the next call.
    file_content_response = get_content(
        org=github_organisation,
        repo=github_repository,
        branch=_github_page_branch,
        repo_path=f"docs/elementary/{environment}/elementary_report.html",
        http_header=github_http_header,
    )

    sha = None
    if file_content_response:
        sha = file_content_response.get("sha")
    logger.debug(f"Elementary report already exists and has sha: {sha}")

    # Finally, the report is committed to the branch that contains the GitHub Pages
    create_or_update_file_content(
        org=github_organisation,
        repo=github_repository,
        branch=_github_page_branch,
        repo_path=f"docs/elementary/{environment}",
        source_path=f"{report_path}/elementary_report.html",
        commit_message=f"Updated 'Elementary report' for '{environment}'",
        sha=sha,
        http_header=github_http_header,
    )
    logger.info(f"Updated 'Elementary report' for '{environment}.")


def run_edr_monitor():
    """
    Runs the edr monitor command with the specified
    Teams webhook URL from the elementary context.
    """
    # Extract the Teams webhook URL from the github variables
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    # Construct the command to run the edr monitor with the Teams webhook URL
    command = [
        "edr",
        "monitor",
        "--teams-webhook",
        webhook_url,
    ]
    # Now run the command, capture the output and handle potential errors.
    try:
        run_result = run(
            command,
            capture_output=True,
        )
        run_result.check_returncode()
    except CalledProcessError as ex:
        if ex.stderr != b"":
            logger.error(ex.stderr.decode("utf-8"))
        elif ex.stdout != b"":
            logger.error(ex.stdout.decode("utf-8"))
        raise

    logger.info("Elementary notification report successfully generated.")
