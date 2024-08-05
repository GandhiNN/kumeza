class JDBCManager:

    def __init__(
        self, hostname: str, port: str, db_instance: str, domain: str
    ):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.domain = domain

    def get_connection_string(self, db_type: str, db_name: str) -> str:
        match db_type:
            case "mssql":
                return (
                    """jdbc:jtds:sqlserver://"""
                    f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                    f"""databaseName={db_name};integratedSecurity=false"""
                )
            case "mssql-ntlm":
                return (
                    """jdbc:jtds:sqlserver://"""
                    f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                    f"""databaseName={db_name};integratedSecurity=true;"""
                    f"""useNTLMv2=true;domain={self.domain}"""
                )
            case "postgresql":
                return (
                    """jdbc:jtds:sqlserver://"""
                    f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                    f"""databaseName={db_name};integratedSecurity=true;"""
                    f"""useNTLMv2=true;domain={self.domain}"""
                )
            case "oracle":
                return f"""jdbc:oracle:thin:@{self.hostname}:{self.port}:{self.db_instance}"""
            case "mysql":
                return (
                    f"""jdbc:mysql://{self.hostname}:{self.port}/{db_name}"""
                    """?zeroDateTimeBehavior=CONVERT_TO_NULL&autoCommit=false"""
                )
            case _:
                raise ValueError(f"{db_type}: Database type is not implemented!")

    def get_driver(self, db_engine: str) -> str:
        match db_engine:
            case "postgresql" | "mssql" | "mssql-ntlm":
                return "net.sourceforge.jtds.jdbc.Driver"
            case "oracle":
                return "oracle.jdbc.driver.OracleDriver"
            case "mysql":
                return "com.mysql.cj.jdbc.Driver"
            case _:
                raise ValueError(f"{db_engine}: Database Engine is not Implemented!")
