from typing import Any

import pyarrow as pa


class ArrowConverter:

    def from_python_list(self, result_sets: list[tuple[Any, ...]]) -> pa.Table:
        pass
