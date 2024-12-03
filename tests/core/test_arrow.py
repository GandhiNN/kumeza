import os
import unittest

import pyarrow as pa
import pytest

from kumeza.core.arrow import ArrowIO, ArrowUtils


# raise unittest.SkipTest("##TODO")

test_data = [
    {"col1": 1, "col2": "a"},
    {"col1": 2, "col2": "b"},
    {"col1": 3, "col2": "c"},
    {"col1": 4, "col2": "d"},
    {"col1": 5, "col2": "e"},
]

# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
CFG_PATH_CSV = "files/csv"
CFG_PATH_PARQUET_SINGLE = "files/parquet/single"
CFG_PATH_PARQUET_MULTIPLE = "files/parquet/multiple"
CFG_PATH_TXT = "files/txt"
TESTFILE_CSV = os.path.join(ABS_PATH, CFG_PATH_CSV, "test_arrow.csv")
TESTFILE_PARQUET_SINGLE = os.path.join(
    ABS_PATH, CFG_PATH_PARQUET_SINGLE, "test_arrow.parquet"
)
TESTFILE_TXT = os.path.join(ABS_PATH, CFG_PATH_TXT, "test_arrow.txt")
TESTFILE_PARQUET_MULTIPLE = os.path.join(ABS_PATH, CFG_PATH_PARQUET_MULTIPLE)


class ArrowUtilsTest(unittest.TestCase):
    def setUp(self):
        self.arrow_utils = ArrowUtils()

    def test_convert_python_list_into_arrow(self):
        assert isinstance(
            self.arrow_utils.convert_python_list_to_arrow(test_data), pa.Table
        )

    def test_get_schema(self):
        pass

    def test_convert_int64_to_timestamp(self):
        pass

    def test_get_row_count(self):
        arrow_io = ArrowIO()
        table = arrow_io.read(source_type="parquet", path=TESTFILE_PARQUET_SINGLE)
        assert self.arrow_utils.get_num_rows(table) == 1000


class ArrowIOTest(unittest.TestCase):
    def setUp(self):
        self.arrow_io = ArrowIO()

    def test_read_csv_file_into_arrow(self):
        assert isinstance(
            self.arrow_io.read(source_type="csv", path=TESTFILE_CSV), pa.Table
        )

    def test_read_parquet_file_into_arrow(self):
        assert isinstance(
            self.arrow_io.read(source_type="parquet", path=TESTFILE_PARQUET_SINGLE),
            pa.Table,
        )

    def test_read_parquet_files_from_directory(self):
        assert isinstance(
            self.arrow_io.read(source_type="parquet", path=TESTFILE_PARQUET_MULTIPLE),
            pa.Table,
        )

    def test_read_unrecognized_format_into_arrow(self):
        with pytest.raises(ValueError):
            self.arrow_io.read(source_type="txt", path=TESTFILE_TXT)
