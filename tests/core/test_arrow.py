import os
import unittest

import pyarrow as pa

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
CFG_PATH = "files"
TESTFILE = os.path.join(ABS_PATH, CFG_PATH, "test_arrow.csv")


class ArrowUtilsTest(unittest.TestCase):
    def setUp(self):
        self.arrow_utils = ArrowUtils()

    def test_convert_python_list_into_arrow(self):
        assert isinstance(
            self.arrow_utils.convert_python_list_to_arrow(test_data), pa.Table
        )


class ArrowIOTest(unittest.TestCase):
    def setUp(self):
        self.arrow_io = ArrowIO()

    def test_read_csv_file_into_arrow(self):
        assert isinstance(
            self.arrow_io.read(source_type="csv", path=TESTFILE), pa.Table
        )
