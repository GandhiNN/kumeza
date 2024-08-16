class TDSManager:

    def __init__(self, hostname: str, port: str, db_instance: str):
        self.hostname = hostname
        self.port = port
        self.db_instance = db_instance

    def get_connection_string(self) -> str:
        return f"{self.hostname}:{self.port}\\{self.db_instance}"
