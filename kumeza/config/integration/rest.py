import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class RestConfig(BaseConfig):
    header: dict
    params: dict

    @classmethod
    def marshal(cls: t.Type["RestConfig"], obj: dict):
        return cls(
            header=obj["header"],
            params=obj["params"],
        )
