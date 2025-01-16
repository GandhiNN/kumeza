import json
import logging
import os
import unittest

import pytest

from kumeza.pipeline.metadata import Writer


logger = logging.getLogger(__name__)

# raise unittest.SkipTest("##TODO")

TEST_DATA = [
    {"col1": 1, "col2": "a"},
    {"col1": 2, "col2": "b"},
    {"col1": 3, "col2": "c"},
    {"col1": 4, "col2": "d"},
    {"col1": 5, "col2": "e"},
]

TEST_CSV = """col1,col2
1,a
2,b
3,c
4,d
5,e
"""


class MetadataWriterTest(unittest.TestCase):
    def setUp(self):
        self.metadata_writer = Writer()

    def test_write_metadata_to_csv(self):
        fout = os.path.join(os.path.dirname(__file__), "files", "out.csv")
        self.metadata_writer.write_to_file(
            metadata=TEST_DATA, file_format="csv", output_file=fout
        )
        # read the output file and compare it with the test data
        with open(fout, "r", encoding="utf8") as f:
            data = f.read()
            assert data == TEST_CSV

    def test_write_metadata_to_json(self):
        fout = os.path.join(os.path.dirname(__file__), "files", "out.json")
        self.metadata_writer.write_to_file(
            metadata=TEST_DATA, file_format="json", output_file=fout
        )
        # read the output file and compare it with the test data
        with open(fout, "r", encoding="utf8") as f:
            data = f.read()
            assert json.loads(data) == TEST_DATA

    def test_read_unrecognized_format(self):
        fout = os.path.join(os.path.dirname(__file__), "files", "out.parquet")
        with pytest.raises(ValueError):
            self.metadata_writer.write_to_file(
                metadata=TEST_DATA, file_format="parquet", output_file=fout
            )
