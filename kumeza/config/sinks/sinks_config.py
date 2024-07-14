import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class Sinks:
    id: str
    target: str
    file_format: str
    path: str

    @classmethod
    def marshal(cls: t.Type["Sinks"], obj: dict):
        return cls(
            id=obj["id"],
            target=obj["target"],
            file_format=obj["file_format"],
            path=obj["path"],
        )


@dataclass
class SinksConfig:
    targets: t.Sequence[Sinks]

    @classmethod
    def marshal(cls: t.Type["SinksConfig"], obj: list):
        return cls(targets=[Sinks.marshal(item) for item in obj])
