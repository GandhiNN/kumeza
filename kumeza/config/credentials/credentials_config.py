import typing as t
from dataclasses import dataclass


@dataclass
class CredentialsConfig:
    username: str
    provider: str
    url: str
    verify_ssl: bool
    namespace: str
    mount_point: str
    path: str

    @classmethod
    def marshal(cls: t.Type["CredentialsConfig"], obj: dict):
        return cls(
            username=obj["username"],
            provider=obj["provider"],
            url=obj["url"],
            verify_ssl=obj["verify_ssl"],
            namespace=obj["namespace"],
            mount_point=obj["mount_point"],
            path=obj["path"],
        )
