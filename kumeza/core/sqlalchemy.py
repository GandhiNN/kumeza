from typing import Any

from sqlalchemy.engine import URL, create_engine


class Engine:
    def __init__(self, dialect: str, driver: str):
        self.dialect = dialect
        self.driver = driver

    def create_url(
        self,
        sql_engine: str,
        driver: str,
        username: str,
        password: str,
        host: str,
        port: str,
        database: str,
        domain: str,
    ):
        return URL.create(
            f"{sql_engine}+{driver}",
            username=rf"{domain}\{username}",
            password=rf"{password}",  # pragma: allowlist-secret
            host=rf"{host}:{port}",
            database=rf"{database}",
        )

    def create_engine(self, url: str) -> Any:
        return create_engine(url)
