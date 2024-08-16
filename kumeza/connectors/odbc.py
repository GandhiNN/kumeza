class ODBCManager:

    def __init__(
        self, hostname: str, port: str, db_instance: str, domain: str, db_engine: str
    ):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.domain = domain
        self.driver = self.get_driver(db_engine)

    def get_connection_string(
        self, db_engine: str, db_name: str, uid: str, username: str, password: str
    ) -> str:
        if "mssql" in db_engine:
            return (
                f"DRIVER={self.driver};SERVER={self.hostname};PORT={self.port};"
                f"DATABASE={db_name};UID={uid}\\{username};PWD={password};"
                f"DOMAIN={self.domain};IntegratedSecurity=True;"
                f"TrustServerCertificate=Yes;TrustedConnection=No"
            )
        raise ValueError(f"{db_engine}: Database type is not implemented!")

    def get_driver(self, db_engine: str) -> str:
        if "mssql" in db_engine:
            return "FreeTDS"
        raise ValueError(f"{db_engine}: Database type is not implemented!")
