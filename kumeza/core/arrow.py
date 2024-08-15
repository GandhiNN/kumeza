from typing import Any

import pyarrow as pa


class ArrowConverter:

    def from_python_list(self, result_sets: list[dict[str, Any]]) -> pa.Table:
        return pa.Table.from_pylist(result_sets)
