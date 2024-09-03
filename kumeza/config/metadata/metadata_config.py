import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class Metadata(BaseConfig):
    metadata_type: str
    sink_target: str
    table_name: str
    partition_key: str
    sort_key: str

    @classmethod
    def marshal(cls: t.Type["Metadata"], obj: dict):
        return cls(
            metadata_type=obj["metadata_type"],
            sink_target=obj["sink_target"],
            table_name=obj["table_name"],
            partition_key=obj["partition_key"],
            sort_key=obj["sort_key"],
        )


@dataclass
class MetadataConfig:
    metadata_targets: t.Sequence[Metadata]

    @classmethod
    def marshal(cls: t.Type["MetadataConfig"], obj: list):
        return cls(
            metadata_targets=[Metadata.marshal(item) for item in obj],
        )
