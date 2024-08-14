class JDBCManager:

    def __init__(
        self, hostname: str, port: str, db_instance: str, domain: str, db_engine: str
    ):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.domain = domain
        self.db_engine = db_engine
        self.driver = self._get_driver(self.db_engine)

    def get_connection_string(self, db_engine: str, db_name: str) -> str:
        if db_engine == "mssql":
            return (
                """jdbc:jtds:sqlserver://"""
                f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                f"""databaseName={db_name};integratedSecurity=false"""
            )
        if db_engine in ("postgresql", "mssql-ntlm"):
            return (
                """jdbc:jtds:sqlserver://"""
                f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                f"""databaseName={db_name};integratedSecurity=true;"""
                f"""useNTLMv2=true;domain={self.domain}"""
            )
        if db_engine == "oracle":
            return (
                f"""jdbc:oracle:thin:@{self.hostname}:{self.port}:{self.db_instance}"""
            )
        if db_engine == "mysql":
            return (
                f"""jdbc:mysql://{self.hostname}:{self.port}/{db_name}"""
                """?zeroDateTimeBehavior=CONVERT_TO_NULL&autoCommit=false"""
            )
        raise ValueError(f"{db_engine}: Database type is not implemented!")

    def _get_driver(self, db_engine: str) -> str:
        if db_engine == "postgresql" or "mssql" in db_engine:
            return "net.sourceforge.jtds.jdbc.Driver"
        elif db_engine == "oracle":
            return "oracle.jdbc.driver.OracleDriver"
        elif db_engine == "mysql":
            return "com.mysql.cj.jdbc.Driver"
        else:
            raise ValueError(f"{db_engine}: Database Engine is not Implemented!")
