import logging
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from kumeza.core.data import ArrowToHiveMapping


log = logging.getLogger(__name__)


class ArrowConverter:

    def from_python_list(self, result_sets: list[dict[str, Any]]) -> pa.Table:
        log.info("Converting input to PyArrow table")
        return pa.Table.from_pylist(result_sets)


class ArrowManager:  # pragma: no cover

    @classmethod
    def get_schema(cls, table: pa.Table, hive: bool = False) -> list[dict]:
        schema = []
        log.info("Getting table schema from Arrow table. Hive mapping = %s", hive)
        for field in table.schema:
            s = {
                "name": str(field.name),
                "type": (
                    ArrowToHiveMapping.transform_schema(str(field.type))
                    if hive
                    else str(field.type)
                ),
                "description": str(field.metadata),
                "nullable": str(field.nullable),
            }
            schema.append(s)
        return schema

    @classmethod
    def write_to_s3(cls, table: pa.Table, s3uri: str):
        log.info("Writing Arrow table to %s", s3uri)
        pq.write_to_dataset(table, root_path=s3uri)
