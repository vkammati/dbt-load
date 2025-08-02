from pydantic import Field, PositiveInt
from validation_schema.shared.base_cluster_validation_schema import DbxBaseClusterConfig
from validation_schema.shared.base_model import DbxBaseModel


class DbxClusterConfig(DbxBaseClusterConfig):
    name: str = Field(pattern=r"^[a-zA-Z0-9_]*$")
    autotermination_minutes: PositiveInt | None = None
    python_packages: list[str] | None = None


class DbxClustersConfig(DbxBaseModel):
    cluster: list[DbxClusterConfig]
