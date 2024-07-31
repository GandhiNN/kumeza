import pyspark

from kumeza.connectors.jdbc import JDBCManager
from kumeza.connectors.spark import SparkManager


class SparkExtractor:

    def __init__(self, sparkmanager: SparkManager, jdbcmanager: JDBCManager):
        self.sparkmanager = sparkmanager
        self.jdbcmanager = jdbcmanager

    def set_spark_debug_level(self, level: str = "DEBUG"):
        self.sparkmanager.session.SparkContext.setLogLevel(level)

    def use_proleptic_gregorian_calendar(self):
        """
        # https://issues.apache.org/jira/browse/SPARK-31408
        # https://stackoverflow.com/questions/69745427/pyspark-outputting-01-01-0001-and-12-31-9999-incorrectly-in-parquet
        #"""
        self.sparkmanager.session.conf.setAll(
            [
                ("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED"),
                ("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED"),
                ("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED"),
                ("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED"),
                ("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS"),
            ]
        )

    def read(
        self, db_type: str, sqlquery: str, username: str, password: str
    ) -> pyspark.sql.DataFrame:
        return (
            self.sparkmanager.spark.read.format("jdbc")
            .option("url", self.jdbcmanager.get_connection_string(db_type))
            .option("driver", self.jdbcmanager.get_driver(db_type))
            .option("fetchsize", 1e6)
            .option("user", username)
            .option("password", password)
            .option("query", sqlquery)
            .load()
        )
