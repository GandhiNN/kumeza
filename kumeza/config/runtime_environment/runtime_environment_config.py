import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class RuntimeEnvironmentConfig(BaseConfig):
    id: str
    provider: str
    service: str
    region: str
    env: str

    @classmethod
    def marshal(cls: t.Type["RuntimeEnvironmentConfig"], obj: dict):
        return cls(
            id=obj["id"],
            provider=obj["provider"],
            service=obj["service"],
            region=obj["region"],
            env=obj["env"],
        )
