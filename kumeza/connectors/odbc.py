class ODBCManager:

    def __init__(
        self, hostname: str, port: str, db_instance: str, db_name: str, domain: str
    ):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.db_name = db_name
        self.domain = domain

    def get_connection_string(
        self, db_type: str, uid: str, username: str, password: str
    ) -> str:
        match db_type:
            case "mssql":
                return (
                    f"DRIVER={self.get_driver(db_type)};SERVER={self.hostname};PORT={self.port};"
                    f"DATABASE={self.db_name};UID={uid}\\{username};PWD={password};"
                    f"DOMAIN={self.domain};IntegratedSecurity=True;"
                    f"TrustServerCertificate=Yes;TrustedConnection=No"
                )
            case _:
                raise ValueError(f"{db_type}: Database type is not implemented!")

    def get_driver(self, db_type: str) -> str:
        match db_type:
            case "mssql":
                return "FreeTDS"
            case _:
                raise ValueError(f"{db_type}: Database type is not implemented!")
