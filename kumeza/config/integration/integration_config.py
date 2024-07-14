import typing as t
from dataclasses import dataclass

from kumeza.config import BaseConfig


@dataclass
class IntegrationConfig(BaseConfig):
    engine: str
    driver: str
    fetchsize: int
    chunksize: int

    @classmethod
    def marshal(cls: t.Type["IntegrationConfig"], obj: dict):
        return cls(
            engine=obj["engine"],
            driver=obj["driver"],
            fetchsize=obj["fetchsize"],
            chunksize=obj["chunksize"],
        )
