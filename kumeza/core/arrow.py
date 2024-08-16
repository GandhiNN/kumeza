from typing import Any

import pyarrow as pa


class ArrowConverter:

    def from_python_list(self, result_sets: list[dict[str, Any]]) -> pa.Table:
        return pa.Table.from_pylist(result_sets)


class ArrowManager:

    @classmethod
    def get_schema(cls, table: pa.Table) -> dict:
        schema = {}
        for field in table:
            schema[str(field.name)] = str(field.type)
        return schema
