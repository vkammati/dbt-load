from pydantic import BaseModel


class DbxBaseModel(BaseModel):
    class Config:
        extra = "forbid"
