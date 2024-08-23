import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class SinkTargets(BaseConfig):
    id: str
    target: str
    file_format: str
    path: str

    @classmethod
    def marshal(cls: t.Type["SinkTargets"], obj: dict):
        return cls(
            id=obj["id"],
            target=obj["target"],
            file_format=obj["file_format"],
            path=obj["path"],
        )


@dataclass
class Sinks(BaseConfig):
    sink_type: str
    sink_targets: t.Sequence[SinkTargets]

    @classmethod
    def marshal(cls: t.Type["Sinks"], obj: dict):
        return cls(
            sink_type=obj["sink_type"],
            sink_targets=[SinkTargets.marshal(item) for item in obj["sink_targets"]],
        )

    def get_sink_target(self, target_id: str):
        for target in self.sink_targets:
            if target.id == target_id:
                return target


@dataclass
class SinksConfig:
    sink_type: t.Sequence[Sinks]

    @classmethod
    def marshal(cls: t.Type["SinksConfig"], obj: list):
        return cls(sink_type=[Sinks.marshal(item) for item in obj])

    def get_sink(self, sink_type: str):
        for sink in self.sink_type:
            if sink.sink_type == sink_type:
                return sink
