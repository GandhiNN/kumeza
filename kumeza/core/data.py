import hashlib
import json


def get_schema_hash(schema: list[dict]) -> str:
    return hashlib.sha256(json.dumps(schema, indent=4).encode("utf-8")).hexdigest()


class SparkToHiveMapping:

    def __init__(self):
        self.dtype_mapping = {
            "StringType": "string",
            "IntegerType": "int",
            "integer": "int",
            "TimestampType": "string",
            "timestamp": "string",
            "BooleanType": "string",
            "LongType": "bigint",
            "long": "bigint",
            "DoubleType": "double",
            "BinaryType": "string",
            "DateType": "string",
            "DecimalType": "double",
        }

    def transform_schema(self, spark_dtype: str):
        pass


class ArrowToHiveMapping:
    dtype_mapping = {
        "int64": "int",
        "decimal128(19, 10)": "float",
        "timestamp[us]": "string",
        "string": "string",
        "bool": "string",
        "null": "string",
    }

    @classmethod
    def transform_schema(cls, arrow_dtype: str):
        try:
            return cls.dtype_mapping[arrow_dtype]
        except Exception as e:
            raise ValueError(f"{arrow_dtype} is not implemented yet") from e
