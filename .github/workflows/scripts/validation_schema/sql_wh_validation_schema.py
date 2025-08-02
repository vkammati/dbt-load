from typing import Literal

from pydantic import Field, PositiveInt
from validation_schema.shared.base_model import DbxBaseModel


class DbxSqlEndPointAccessConfig(DbxBaseModel):
    group_name: str | None = None
    permission_level: (
        Literal["CAN_MANAGE", "CAN_USE", "CAN_MONITOR", "IS_OWNER"] | None
    ) = None


class DbxSqlEndPointTagsConfig(DbxBaseModel):
    key: str | None = None
    value: str | None = None


class DbxSqlWarehouseConfig(DbxBaseModel):
    name: str = Field(pattern=r"^[a-zA-Z0-9_]*$")
    cluster_size: str | None = None
    warehouse_type: str | None = None
    enable_serverless_compute: bool | None = None
    min_num_clusters: PositiveInt | None = None
    max_num_clusters: PositiveInt | None = None
    auto_stop_mins: int | None = None
    access_control: list[DbxSqlEndPointAccessConfig] | None = None
    tags: list[DbxSqlEndPointTagsConfig] | None = None


class DbxSqlWarehousesConfig(DbxBaseModel):
    sql_warehouse: list[DbxSqlWarehouseConfig]
