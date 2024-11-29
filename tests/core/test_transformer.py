import string
import unittest

# raise unittest.SkipTest("##TODO")
import pytest

from kumeza.core.transformer import (
    ArrowToHiveMapping,
    SparkToHiveMapping,
    get_schema_hash,
)


test_schema = [
    {"name": "id", "type": "int", "description": "id"},
    {"name": "item", "type": "string", "description": "item name"},
    {
        "name": "price",
        "type": "float",
        "description": "item price",
    },
]


class TestTransformerUtils(unittest.TestCase):
    def setUp(self):
        self.hexstr = get_schema_hash(test_schema)

    def test_get_schema_hash(self):
        # assert if the resultant is a string type
        assert isinstance(self.hexstr, str)

        # assert if the resultant contains only hex digits (0...9, A...F)
        hex_digits_set = set(string.hexdigits)
        # if self.hexdigits is long, it's faster to chec against a set
        assert all(c in hex_digits_set for c in self.hexstr) is True


class TestSparkToHiveTransformer(unittest.TestCase):
    def setUp(self):
        self.mapping = SparkToHiveMapping()

    def test_transform_stringtype_to_string(self):
        assert self.mapping.transform_schema(spark_dtype="StringType") == "string"

    def test_transform_integertype_to_string(self):
        assert self.mapping.transform_schema(spark_dtype="IntegerType") == "int"

    def test_transform_integer_to_int(self):
        assert self.mapping.transform_schema(spark_dtype="integer") == "int"

    def test_transform_timestamptype_to_string(self):
        assert self.mapping.transform_schema(spark_dtype="TimestampType") == "string"

    def test_transform_timestamp_to_string(self):
        assert self.mapping.transform_schema(spark_dtype="timestamp") == "string"

    def test_transform_booleantype_to_string(self):
        assert self.mapping.transform_schema(spark_dtype="BooleanType") == "string"

    def test_transform_longtype_to_bigint(self):
        assert self.mapping.transform_schema(spark_dtype="LongType") == "bigint"

    def test_transform_long_to_bigint(self):
        assert self.mapping.transform_schema(spark_dtype="long") == "bigint"

    def test_transform_doubletype_to_double(self):
        assert self.mapping.transform_schema(spark_dtype="DoubleType") == "double"

    def test_transform_binarytype_to_double(self):
        assert self.mapping.transform_schema(spark_dtype="BinaryType") == "string"

    def test_transform_datetype_to_string(self):
        assert self.mapping.transform_schema(spark_dtype="DateType") == "string"

    def test_transform_decimaltype_to_double(self):
        assert self.mapping.transform_schema(spark_dtype="DecimalType") == "double"

    def test_transform_decimal_to_float(self):
        assert self.mapping.transform_schema(spark_dtype="decimal") == "float"

    def test_unimplemented_spark_type(self):
        with pytest.raises(ValueError):
            self.mapping.transform_schema(spark_dtype="blob")


class TestArrowToHiveTransformer(unittest.TestCase):
    def setUp(self):
        self.mapping = ArrowToHiveMapping()

    def test_transform_int64_to_int(self):
        assert self.mapping.transform_schema(arrow_dtype="int64") == "int"

    def test_transform_timestamp_to_string(self):
        assert self.mapping.transform_schema(arrow_dtype="timestamp[us]") == "string"

    def test_transform_string(self):
        assert self.mapping.transform_schema(arrow_dtype="string") == "string"

    def test_transform_bool_to_string(self):
        assert self.mapping.transform_schema(arrow_dtype="bool") == "string"

    def test_transform_null_to_string(self):
        assert self.mapping.transform_schema(arrow_dtype="null") == "string"

    def test_transform_decimal_to_float(self):
        assert self.mapping.transform_schema(arrow_dtype="decimal") == "float"

    def test_unimplemented_arrow_type(self):
        with pytest.raises(ValueError):
            self.mapping.transform_schema(arrow_dtype="blob")
