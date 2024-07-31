import pyspark

from kumeza.connectors.jdbc import JDBCManager
from kumeza.connectors.spark import SparkManager

from kumeza import StaticFiles

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
        self,
        db_engine: str,
        sqlquery: str,
        username: str,
        password: str,
        use_proleptic_gregorian_calendar: bool = True,
    ) -> pyspark.sql.DataFrame:
        if use_proleptic_gregorian_calendar:
            self.use_proleptic_gregorian_calendar()
        match db_engine:
            case "mssql"|"mssql-ntlm"|"postgresql":
                self.sparkmanager.session.conf.set("spark.jars", StaticFiles.jtds_jar)
                return (
                    self.sparkmanager.session.read.format("jdbc")
                    .option("url", self.jdbcmanager.get_connection_string(db_engine))
                    .option("driver", self.jdbcmanager.get_driver(db_engine))
                    .option("fetchsize", 1e6)
                    .option("user", username)
                    .option("password", password)
                    .option("query", sqlquery)
                    .load()
                )
            case "oracle":
                self.sparkmanager.session.conf.set("spark.jars", StaticFiles.oracle_jar)
                return (
                    self.sparkmanager.session.read.format("jdbc")
                    .option("url", self.jdbcmanager.get_connection_string(db_engine))
                    .option("driver", self.jdbcmanager.get_driver(db_engine))
                    .option("fetchsize", 1e6)
                    .option("user", username)
                    .option("password", password)
                    .option("dbtable", f"({sqlquery})")
                    .load()
                )
            case "mysql":
                self.sparkmanager.session.conf.set("spark.jars", StaticFiles.mysql_jar)
                return (
                    self.sparkmanager.session.read.format("jdbc")
                    .option("url", self.jdbcmanager.get_connection_string(db_engine))
                    .option("driver", self.jdbcmanager.get_driver(db_engine))
                    .option("fetchsize", 1e6)
                    .option("user", username)
                    .option("password", password)
                    .option("dbtable", f"({sqlquery}) foo")
                    .load()
                )
            case _:
                raise ValueError(f"{db_engine}: Database Engine is not Implemented!")

