import logging
from typing import Any

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


logger = logging.getLogger(__name__)


class Engine:
    def __init__(self, engine_type: str):
        if engine_type == "polars":
            self.engine = pl
        else:
            raise ValueError(f"{engine_type} not implemented yet")


class IO(Engine):
    def __init__(self, engine: str):
        super().__init__(engine_type=engine)


class DeltaLakeManager:
    def __init__(self):
        self.data = None

    def read(self, path: str):
        self.data = DeltaTable(path)

    def write(self, path: str, df: Any):
        write_deltalake(path, df)

    def predicate_builder(
        self,
        business_keys: list[dict],
        source_alias: str = "s",
        target_alias: str = "t",
    ) -> list[str]:
        tmp = []
        for item in business_keys:
            stmt = (
                f"""{source_alias}.{item["'col'"]} """
                f"""{item["'condition'"]} """
                f"""{target_alias}.{item["'col'"]}"""
            )
            tmp.append(stmt)
        return tmp

    def merge_and_upsert(self, new_data: pa.Table, business_keys: list, source_alias):
        pass  # TODO
