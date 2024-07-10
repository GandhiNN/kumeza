import typing as t
from dataclasses import dataclass


@dataclass
class CredentialsConfig:
    provider: str
    workspace: str
    mount_point: str
    path: str

    @classmethod
    def marshal(cls: t.Type["CredentialsConfig"], obj: dict):
        return cls(
            provider=obj["provider"],
            workspace=obj["workspace"],
            mount_point=obj["mount_point"],
            path=obj["path"],
        )
