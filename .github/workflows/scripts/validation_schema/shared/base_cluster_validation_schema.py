from typing import Literal

from pydantic import NonNegativeInt, PositiveInt, model_validator
from typing_extensions import Self
from validation_schema.shared.base_model import DbxBaseModel
from validation_schema.shared.helpers import check_mutual_exclusive


class DbxClusterAzureConfig(DbxBaseModel):
    first_on_demand: int | None = None
    availability: (
        Literal["SPOT_AZURE", "SPOT_WITH_FALLBACK_AZURE", "ON_DEMAND_AZURE"] | None
    ) = None
    spot_bid_max_price: int | None = None


class DbxClusterAccessConfig(DbxBaseModel):
    group_name: str | None = None
    permission_level: Literal["CAN_ATTACH_TO", "CAN_MANAGE", "CAN_RESTART"] | None = None


class DbxClusterAwsConfig(DbxBaseModel):
    first_on_demand: PositiveInt | None = None
    availability: Literal["SPOT", "SPOT_WITH_FALLBACK", "ON_DEMAND"] | None = None
    spot_bid_price_percent: PositiveInt | None = None


class DbxClusterAutoscaleConfig(DbxBaseModel):
    min_workers: PositiveInt | None = None
    max_workers: PositiveInt | None = None


class DbxBaseClusterConfig(DbxBaseModel):
    spark_version: str | None = None
    node_type_id: str | None = None
    runtime_engine: Literal["STANDARD", "PHOTON"] | None = None
    data_security_mode: (
        Literal["SINGLE_USER", "USER_ISOLATION", "LEGACY_PASSTHROUGH", "LEGACY_TABLE_ACL"]
        | None
    ) = None
    num_workers: NonNegativeInt | None = None
    autoscale: DbxClusterAutoscaleConfig | None = None
    spark_env_vars: dict[str, str] | None = None
    azure_attributes: DbxClusterAzureConfig | None = None
    aws_attributes: DbxClusterAwsConfig | None = None
    access_control: list[DbxClusterAccessConfig] | None = None

    @model_validator(mode="after")
    def check_only_one_workers(self) -> Self:
        check_mutual_exclusive(self, ["num_workers", "autoscale"])
        return self

    @model_validator(mode="after")
    def check_only_one_cloud_setting(self) -> Self:
        check_mutual_exclusive(self, ["azure_attributes", "aws_attributes"])
        return self
