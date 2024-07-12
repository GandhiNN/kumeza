import typing as t
from dataclasses import dataclass


@dataclass
class SourceSystemConfig:
    id: str
    database_type: str
    database_instance: str
    authentication_type: str
    hostname: str
    domain: str
    port: int

    @classmethod
    def marshal(cls: t.Type["SourceSystemConfig"], obj: dict):
        return cls(
            id=obj["id"],
            database_type=obj["database_type"],
            database_instance=obj["database_instance"],
            authentication_type=obj["authentication_type"],
            hostname=obj["hostname"],
            domain=obj["domain"],
            port=obj["port"],
        )
