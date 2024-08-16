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

    def transform_schema(self, spark_schema: dict):
        pass
