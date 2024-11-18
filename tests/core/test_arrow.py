import os
import unittest

import pyarrow as pa

from kumeza.core.arrow import ArrowConverter


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


class ArrowConverterTest(unittest.TestCase):
    def setUp(self):
        self.arrow_converter = ArrowConverter()

    def test_convert_python_list_into_arrow(self):
        assert isinstance(self.arrow_converter.from_python_list(test_data), pa.Table)

    def test_convert_csv_file_into_arrow(self):
        assert isinstance(self.arrow_converter.from_csv(TESTFILE), pa.Table)
