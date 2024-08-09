import pyarrow
from arrow_odbc import read_arrow_batches_from_odbc

from kumeza.connectors.odbc import ODBCManager


class ArrowExtractor:

    def __init__(self, odbcmanager: ODBCManager):
        self.odbcmanager = odbcmanager

    # TODO: Python 3.10
    # def read(
    #     self,
    #     db_engine: str,
    #     db_name: str,
    #     sqlquery: str,
    #     username: str,
    #     password: str,
    #     uid: str,
    #     fetchsize: int = 1000000,
    #     concurrent: bool = False,
    # ) -> pyarrow.RecordBatch:
    #     match db_engine:
    #         case "mssql" | "mssql-ntlm":
    #             reader = read_arrow_batches_from_odbc(
    #                 query=sqlquery,
    #                 connection_string=self.odbcmanager.get_connection_string(
    #                     db_engine, db_name, uid, username, password
    #                 ),
    #                 batch_size=fetchsize,
    #                 user=username,
    #                 password=password,
    #             )
    #             if concurrent:
    #                 reader.fetch_concurrently()
    #             return reader
    #         case _:
    #             raise ValueError(f"{db_engine}: Database Engine is not implemented!")

    def read(
        self,
        db_engine: str,
        db_name: str,
        sqlquery: str,
        username: str,
        password: str,
        uid: str,
        fetchsize: int = 1000000,
        concurrent: bool = False,
    ) -> pyarrow.RecordBatch:
        if "mssql" in db_engine:
            reader = read_arrow_batches_from_odbc(
                query=sqlquery,
                connection_string=self.odbcmanager.get_connection_string(
                    db_engine, db_name, uid, username, password
                ),
                batch_size=fetchsize,
                user=username,
                password=password,
            )
            if concurrent:
                reader.fetch_concurrently()
            return reader
        raise ValueError(f"{db_engine}: Database Engine is not implemented!")
