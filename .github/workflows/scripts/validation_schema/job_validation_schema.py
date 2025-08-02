import re
from datetime import date
from typing import Any, Literal, Union

from pydantic import (
    UUID4,
    Discriminator,
    EmailStr,
    PositiveInt,
    Tag,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated, Self
from validation_schema.shared.base_cluster_validation_schema import DbxBaseClusterConfig
from validation_schema.shared.base_model import DbxBaseModel
from validation_schema.shared.helpers import check_mutual_exclusive


class DbxJobNotificationSettings(DbxBaseModel):
    no_alert_for_skipped_runs: bool | None = None
    no_alert_for_canceled_runs: bool | None = None


class DbxJobSchedule(DbxBaseModel):
    quartz_cron_expression: str | None = None
    timezone_id: str | None = None
    pause_status: Literal["PAUSED", "UNPAUSED"] | None = None

    @field_validator("quartz_cron_expression")
    def validate_cron(cls, v):
        if v is not None:
            # Updated Quartz cron regex pattern to better handle lists and special
            # characters
            pattern = (
                r"^(\*|\?|0|[1-5]?\d|L|W|(\d+(\/|-|\#)\d+)|(\d+L)|(\*\/\d+)|"
                r"(\d+(\s*,\s*\d+)*)) "  # Second (0-59, L, W, #, -, /, *, list)
                r"(\*|\?|0|[1-5]?\d|L|W|(\d+(\/|-|\#)\d+)|(\d+L)|(\*\/\d+)|"
                r"(\d+(\s*,\s*\d+)*)) "  # Minute (0-59, L, W, #, -, /, *, list)
                r"(\*|\?|0|1?\d|2[0-3]|L|W|(\d+(\/|-|\#)\d+)|(\d+L)|(\*\/\d+)|"
                r"(\d+(\s*,\s*\d+)*)) "  # Hour (0-23, L, W, #, -, /, *, list)
                # Day of Month (1-31, L, LW, W, -, /, *, list)
                r"(\*|\?|L|LW|W|\d{1,2}|L-\d{1,2}|(\d+(\/|-|\#|L)\d+)|(\d+L)"
                r"(\*\/\d+)|(\d+(\s*,\s*\d+)*)) "
                r"(\*|\?|1?\d|2[0-3]|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|"
                # Month (1-12 or JAN-DEC, -, /, *, list)
                r"(\d+(\/|-|\#)\d+)|(\*\/\d+)|(\d+(\s*,\s*\d+)*)) "
                r"(\*|\?|L|\d{1,2}|MON|TUE|WED|THU|FRI|SAT|SUN|"  # Days of week
                r"(MON|TUE|WED|THU|FRI|SAT|SUN)-(MON|TUE|WED|THU|FRI|SAT|SUN)|"
                r"(\d+(\/|-|\#|L)\d+)|(\d+L)|(\*\/\d+)"
                r"(\d+(\s*,\s*\d+)*))"
                # Year (optional) and optionally allow a trailing *
                r"(\s+\d{4}(-\d{4})?)?(\s+\*)?$"
            )

            if not re.match(pattern, v):
                raise ValueError("Invalid cron expression")
        return v


class DbxJobContinuousConfig(DbxBaseModel):
    pause_status: Literal["PAUSED", "UNPAUSED"] | None = None


class DbxJobTriggerConfig(DbxBaseModel):
    pause_status: Literal["PAUSED", "UNPAUSED"] | None = None
    file_arrival_url: str | None = None
    file_arrival_min_time_between_triggers_seconds: PositiveInt | None = None
    file_arrival_wait_after_last_change_seconds: PositiveInt | None = None


class DbxEmailNotificationsConfig(DbxBaseModel):
    on_failure: list[EmailStr] | None = None
    on_start: list[EmailStr] | None = None
    on_success: list[EmailStr] | None = None
    on_duration_warning_threshold_exceeded: list[EmailStr] | None = None


class DbxWebhookNotificationsConfig(DbxBaseModel):
    on_failure: list[UUID4] | None = None
    on_start: list[UUID4] | None = None
    on_success: list[UUID4] | None = None
    on_duration_warning_threshold_exceeded: list[UUID4] | None = None


class DbxContinuousConfigConfig(DbxBaseModel):
    continuous: bool
    finish_after_seconds: PositiveInt
    finish_after_runs: PositiveInt


class DbxElementaryConfig(DbxBaseModel):
    update_github_pages: bool | None = None
    max_days_back: int | None = None
    earliest_date_back: date | None = None
    teams_notification: bool | None = None


class DbxJobClusterConfig(DbxBaseClusterConfig):
    job_cluster_key: str


class DbxJobBaseTaskConfig(DbxBaseModel):
    task_key: str
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] | None = None
    http_path: str | None = None
    max_retries: int | None = None
    min_retry_interval_millis: PositiveInt | None = None
    timeout_seconds: PositiveInt | None = None
    task_runtime_warning_threshold_seconds: PositiveInt | None = None
    depends_on: list[str] | None = None
    run_if: (
        Literal[
            "ALL_SUCCESS",
            "AT_LEAST_ONE_SUCCESS",
            "NONE_FAILED",
            "ALL_DONE",
            "AT_LEAST_ONE_FAILED",
            "ALL_FAILED",
        ]
        | None
    ) = None
    existing_cluster_id: str | None = None
    existing_cluster_name: str | None = None
    job_cluster_key: str | None = None
    email_notifications: DbxEmailNotificationsConfig | None = None
    webhook_notifications: DbxWebhookNotificationsConfig | None = None

    @model_validator(mode="after")
    def check_only_one_cluster_setting(self) -> Self:
        check_mutual_exclusive(
            self, ["existing_cluster_id", "existing_cluster_name", "job_cluster_key"]
        )
        return self


class DbxJobWheelTaskConfig(DbxJobBaseTaskConfig):
    dbt_command: str
    http_path: str | None = None
    package_name: str | None = None
    entry_point: str | None = None
    dbt_vars: dict | None = None
    elementary: DbxElementaryConfig | None = None
    continuous_config: DbxContinuousConfigConfig | None = None
    cluster_type: str | None = None


class DbxJobNotebookTaskConfig(DbxJobBaseTaskConfig):
    notebook_path: str
    parameters: dict[str, str] | None = None
    python_packages: list[str] | None = None


# task can be 'wheel' task or 'notebook' tasks. Pydantic is capable of unions that would
# allow any of the two task and validate. But if there is a valdiation error (unspecified
# field, wrong datatype, etc) it would not be able to tell which task type it is meant to
# be the (because it is incorrect) and would raise hard to understand erros showing
# fields from both types. This can be confusing. To solve this, we use a discriminator
# function that would return the task type based on the required fields. This would allow
# Pydantic to generate a more specific error message. For more info:
# https://docs.pydantic.dev/2.8/concepts/unions/#discriminated-unions-with-callable-discriminator
def get_dbx_task_config_discriminator(v: Any) -> str:
    if v.get("dbt_command"):
        return "wheel"
    elif v.get("notebook_path"):
        return "notebook"
    return "unknown"


class DbxAzureAvailabilityConfig(DbxBaseModel):
    availability: (
        Literal["ON_DEMAND_AZURE", "SPOT_WITH_FALLBACK_AZURE", "SPOT_AZURE"] | None
    ) = None


class DbxAWSAvailabilityConfig(DbxBaseModel):
    availability: Literal["ON_DEMAND", "SPOT_WITH_FALLBACK", "SPOT"] | None = None


class DbxJobClusterAvailabilityConfig(DbxBaseModel):
    azure_attributes: DbxAzureAvailabilityConfig | None = None
    aws_attributes: DbxAWSAvailabilityConfig | None = None


class DbxJobConfig(DbxBaseModel):
    name: str
    tasks: list[
        Annotated[
            Union[
                Annotated[DbxJobWheelTaskConfig, Tag("wheel")],
                Annotated[DbxJobNotebookTaskConfig, Tag("notebook")],
            ],
            Discriminator(get_dbx_task_config_discriminator),
        ]
    ]
    job_clusters: list[DbxJobClusterConfig] | None = None
    trigger_once_after_deploy: bool | None = None
    timeout_seconds: PositiveInt | None = None
    job_runtime_warning_threshold_seconds: PositiveInt | None = None
    max_concurrent_runs: PositiveInt | None = None
    email_notifications: DbxEmailNotificationsConfig | None = None
    webhook_notifications: DbxWebhookNotificationsConfig | None = None
    notification_settings: DbxJobNotificationSettings | None = None
    schedule: DbxJobSchedule | None = None
    tags: dict[str, str] | None = None
    continuous: DbxJobContinuousConfig | None = None
    trigger: DbxJobTriggerConfig | None = None
    queue: bool | None = None
    azure_availability: DbxAzureAvailabilityConfig | None = None
    aws_availability: DbxAWSAvailabilityConfig | None = None
    cluster_type: str | None = None

    @model_validator(mode="after")
    def check_only_one_trigger_setting(self) -> Self:
        check_mutual_exclusive(self, ["schedule", "continuous", "trigger"])
        return self


class DbxJobsConfig(DbxBaseModel):
    jobs: list[DbxJobConfig]
    job_cluster: DbxJobClusterAvailabilityConfig
