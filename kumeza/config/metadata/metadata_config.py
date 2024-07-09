import typing as t

from dataclasses import dataclass


@dataclass
class MetadataConfig:
    sink_type: str
    table_name: str

    @classmethod
    def marshal(cls: t.Type["MetadataConfig"], obj: dict):
        return cls(sink_type=obj["sink_type"], table_name=obj["table_name"])
