from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from kumeza.core.data import ArrowToHiveMapping


class ArrowConverter:

    def from_python_list(self, result_sets: list[dict[str, Any]]) -> pa.Table:
        return pa.Table.from_pylist(result_sets)


class ArrowManager:

    @classmethod
    def get_schema(cls, table: pa.Table, hive: bool = False) -> list[dict]:
        schema = []
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
        pq.write_to_dataset(table, root_path=s3uri)
