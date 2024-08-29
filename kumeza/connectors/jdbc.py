class JDBCManager:

    def __init__(self, hostname: str, port: str, db_instance: str, domain: str):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.domain = domain

    def get_connection_string(
        self, db_engine: str, db_name: str, read_only: bool = True
    ) -> str:
        if db_engine == "mssql":
            if read_only:
                return (
                    """jdbc:jtds:sqlserver://"""
                    f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                    f"""databaseName={db_name};integratedSecurity=false;"""
                    f"""readonly=true"""
                )
            return (
                """jdbc:jtds:sqlserver://"""
                f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                f"""databaseName={db_name};integratedSecurity=false"""
            )
        if db_engine in ("postgresql", "mssql-ntlm"):
            if read_only:
                return (
                    """jdbc:jtds:sqlserver://"""
                    f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                    f"""databaseName={db_name};integratedSecurity=true;"""
                    f"""useNTLMv2=true;domain={self.domain};"""
                    f"""readonly=true"""
                )
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

    def get_driver(self, db_engine: str) -> str:
        if db_engine == "postgresql" or "mssql" in db_engine:
            return "net.sourceforge.jtds.jdbc.Driver"
        if db_engine == "oracle":
            return "oracle.jdbc.driver.OracleDriver"
        if db_engine == "mysql":
            return "com.mysql.cj.jdbc.Driver"
        raise ValueError(f"{db_engine}: Database Engine is not Implemented!")
