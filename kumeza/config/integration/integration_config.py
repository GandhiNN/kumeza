import typing as t
from dataclasses import dataclass


@dataclass
class IntegrationConfig:
    driver: str
    connector: str
    fetchsize: int
    chunksize: int

    @classmethod
    def marshal(cls: t.Type["IntegrationConfig"], obj: dict):
        return cls(
            driver=obj["driver"],
            connector=obj["connector"],
            fetchsize=obj["fetchsize"],
            chunksize=obj["chunksize"],
        )
