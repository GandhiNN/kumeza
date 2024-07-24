class JDBCManager:

    def __init__(self, db_type: str):
        self.db_type = db_type

    def get_connstring(self) -> str:
        pass

    def get_driver(self) -> str:
        pass
