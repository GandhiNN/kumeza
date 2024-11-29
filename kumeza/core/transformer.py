import hashlib
import json


def get_schema_hash(schema: list[dict]) -> str:
    return hashlib.sha256(json.dumps(schema, indent=4).encode("utf-8")).hexdigest()


class SparkToHiveMapping:  # pragma: no-cover
    dtype_mapping = {
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

    @classmethod
    def transform_schema(cls, spark_dtype: str) -> str:
        try:
            if "decimal" in str(spark_dtype):
                return "float"
            return cls.dtype_mapping[spark_dtype]
        except Exception as e:
            raise ValueError(f"{spark_dtype} is not implemented yet") from e


class ArrowToHiveMapping:
    dtype_mapping = {
        "int64": "int",
        "timestamp[us]": "string",
        "string": "string",
        "bool": "string",
        "null": "string",
    }

    @classmethod
    def transform_schema(cls, arrow_dtype: str) -> str:
        try:
            if "decimal" in str(arrow_dtype):
                return "float"
            return cls.dtype_mapping[arrow_dtype]
        except Exception as e:
            raise ValueError(f"{arrow_dtype} is not implemented yet") from e
