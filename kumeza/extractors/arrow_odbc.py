import pyarrow
from arrow_odbc import read_arrow_batches_from_odbc


class ArrowODBCExtractor:

    def __init__(self, username: str, password: str, batch_size: int):
        self.username = username
        self.password = password
        self.batch_size = batch_size

    def read(
        self,
        query: str,
        odbc_connstring: str,
        concurrent: bool = False,
    ) -> pyarrow.RecordBatch:
        reader = read_arrow_batches_from_odbc(
            query=query,
            connection_string=odbc_connstring,
            batch_size=self.batch_size,
            user=self.username,
            password=self.password,
        )
        if concurrent:
            reader.fetch_concurrently()
        return reader
