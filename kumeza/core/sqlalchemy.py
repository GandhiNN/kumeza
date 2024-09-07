import urllib.parse

import sqlalchemy
from sqlalchemy import URL, create_engine


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
    ) -> sqlalchemy.Engine:
        url = URL.create(
            f"{self.dialect}+{self.driver}",
            username=f"{domain}\\{username}",
            password=urllib.parse.quote_plus(password),  # pragma: allowlist-secret
            host=host,
            database=database_name,
            port=port,
        )
        return create_engine(url)
