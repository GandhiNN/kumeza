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
