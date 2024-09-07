from sqlalchemy import URL
from sqlalchemy import create_engine

class Engine:

    def __init__(self, dialect: str, driver: str):
        self.dialect = dialect
        self.driver = driver

    def create_url(self, username: str, password: str, host: str, database_name: str) -> URL:
        return URL.create(
            f"{self.dialect}+{self.driver}",
            username=username,
            password=password, #pragma: allowlist-secret
            host=host,
            database=database_name
        )
    
    def create_engine(self, url: URL):
        return create_engine(url)