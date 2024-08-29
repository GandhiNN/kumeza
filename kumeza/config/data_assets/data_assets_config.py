import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class Assets(BaseConfig):
    asset_name: str
    asset_type: str
    database_name: str
    database_schema: str
    query_type: str
    incremental: bool
    incremental_columns: list
    reload: bool
    partition_columns: list
    columns_to_anonymize: list
    custom_query: str
    custom_schema: dict
    cast_timestamp_columns_to_string: bool

    @classmethod
    def marshal(cls: t.Type["Assets"], obj: dict):
        return cls(
            asset_name=obj["asset_name"],
            asset_type=obj["asset_type"],
            database_name=obj["database_name"],
            database_schema=obj["database_schema"],
            query_type=obj["query_type"],
            incremental=obj["incremental"],
            incremental_columns=obj["incremental_columns"],
            reload=obj["reload"],
            partition_columns=obj["partition_columns"],
            columns_to_anonymize=obj["columns_to_anonymize"],
            custom_query=obj["custom_query"],
            custom_schema=obj["custom_schema"],
            cast_timestamp_columns_to_string=obj["cast_timestamp_columns_to_string"],
        )


@dataclass
class AssetsId(BaseConfig):
    id: str
    assets: t.Sequence[Assets]

    @classmethod
    def marshal(cls: t.Type["AssetsId"], obj: dict):
        return cls(
            id=obj["id"],
            assets=[Assets.marshal(item) for item in obj["assets"]],
        )


@dataclass
class DataAssetsConfig:
    id: t.Sequence[AssetsId]

    @classmethod
    def marshal(cls: t.Type["DataAssetsConfig"], obj: list):
        return cls(id=[AssetsId.marshal(item) for item in obj])
