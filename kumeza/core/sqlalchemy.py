import urllib.parse
from typing import Any

from sqlalchemy.engine import URL, create_engine


class Engine:
    def __init__(self, dialect: str, driver: str):
        self.dialect = dialect
        self.driver = driver

    def create_engine(
        self,
        domain: str,
        username: str,
        password: str,
        host: str,
        port: int,
        database_name: str,
    ) -> Any:
        url = URL.create(
            f"{self.dialect}+{self.driver}",
            username=f"{domain}\\{username}",
            password=urllib.parse.quote_plus(password),  # pragma: allowlist-secret
            host=host,
            database=database_name,
            port=port,
        )
        return create_engine(url)
