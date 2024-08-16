from typing import Any

import pyarrow as pa


class ArrowConverter:

    def from_python_list(self, result_sets: list[dict[str, Any]]) -> pa.Table:
        return pa.Table.from_pylist(result_sets)


class ArrowManager:

    @classmethod
    def get_schema(cls, table: pa.Table) -> list[dict]:
        schema = []
        for field in table.schema:
            s = {
                "name": str(field.name),
                "type": str(field.type),
                "description": str(field.metadata),
                "nullable": str(field.nullable),
            }
            schema.append(s)
        return schema
