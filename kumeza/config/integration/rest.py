import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class RestConfig(BaseConfig):
    base_url: str
    header: dict
    params: dict

    @classmethod
    def marshal(cls: t.Type["RestConfig"], obj: dict):
        return cls(
            base_url=obj["base_url"],
            header=obj["header"],
            params=obj["params"],
        )
