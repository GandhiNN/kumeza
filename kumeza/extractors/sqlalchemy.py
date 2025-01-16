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
        database: str,
    ) -> URL:
        return URL.create(
            f"{sql_engine}+{driver}",
            username=rf"{username}",  # for MSSQL Windows Auth -> $domain\$username
            password=rf"{password}",  # pragma: allowlist-secret
            host=rf"{host}",  # to use port number -> $host:$port
            database=rf"{database}",
        )

    def create_engine(self, url: str) -> Any:
        return create_engine(url)

    def read(self, url: str, query_string: str) -> list:
        with self.create_engine(url) as conn:
            return conn.execute(query_string).fetchall()
