import os
import unittest

from kumeza.compacter.deltalake import DeltaLakeManager


# import deltalake


# import pyarrow as pa
# import pytest


# raise unittest.SkipTest("##TODO")

business_keys_mapping = [
    {"fieldname": "id", "condition": "="},
]

# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
CFG_PATH_PARQUET_SINGLE = "files/parquet/single"
CFG_PATH_PARQUET_MULTIPLE = "files/parquet/multiple"
TESTFILE_PARQUET_SINGLE = os.path.join(
    ABS_PATH, CFG_PATH_PARQUET_SINGLE, "test_arrow.parquet"
)
TESTFILE_PARQUET_MULTIPLE = os.path.join(ABS_PATH, CFG_PATH_PARQUET_MULTIPLE)


class DeltaLakeTest(unittest.TestCase):
    def setUp(self):
        self.deltalake_manager = DeltaLakeManager()

    # def test_read_into_delta_table(self):
    #     assert isinstance(
    #         self.deltalake_manager.read(TESTFILE_PARQUET_SINGLE),
    #         deltalake.table.DeltaTable,
    #     )
