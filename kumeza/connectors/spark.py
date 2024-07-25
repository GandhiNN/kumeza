class SparkManager:

    def __init__(
        self, hostname: str, port: str, db_instance: str, db_name: str, domain: str
    ):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance
        self.db_name = db_name
        self.domain = domain

    def get_connection_string(self, db_type: str) -> str:
        if db_type == "mssql":
            return (
                """jdbc:jtds:sqlserver://"""
                f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                f"""databaseName={self.db_name};integratedSecurity=false"""
            )
        if db_type == "mssql-ntlm":
            return (
                """jdbc:jtds:sqlserver://"""
                f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                f"""databaseName={self.db_name};integratedSecurity=true;"""
                f"""useNTLMv2=true;domain={self.domain}"""
            )
        if db_type == "postgresql":
            return (
                """jdbc:jtds:sqlserver://"""
                f"""{self.hostname}:{self.port};instance={self.db_instance};"""
                f"""databaseName={self.db_name};integratedSecurity=true;"""
                f"""useNTLMv2=true;domain={self.domain}"""
            )
        if db_type == "oracle":
            return (
                f"""jdbc:oracle:thin:@{self.hostname}:{self.port}:{self.db_instance}"""
            )
        if db_type == "mysql":
            return (
                f"""jdbc:mysql://{self.hostname}:{self.port}/{self.db_name}"""
                """?zeroDateTimeBehavior=CONVERT_TO_NULL&autoCommit=false"""
            )
        return None

    def get_driver(self, db_type: str) -> str:
        if db_type == "postgresql" or "mssql" in db_type:
            return "net.sourceforge.jtds.jdbc.Driver"
        if db_type == "oracle":
            return "oracle.jdbc.driver.OracleDriver"
        if db_type == "mysql":
            return "com.mysql.cj.jdbc.Driver"
        return None
