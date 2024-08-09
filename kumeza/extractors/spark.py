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
        self.sparkmanager.session.conf.set(
            "spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED"
        )
        self.sparkmanager.session.conf.set(
            "spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED"
        )
        self.sparkmanager.session.conf.set(
            "spark.sql.legacy.parquet.datetimeRebaseModeInRead",
            "CORRECTED",
        )
        self.sparkmanager.session.conf.set(
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite",
            "CORRECTED",
        )
        self.sparkmanager.session.conf.set(
            "spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS"
        )  # https://stackoverflow.com/questions/69745427/pyspark-outputting-01-01-0001-and-12-31-9999-incorrectly-in-parquet

    # TODO: Python 3.10
    # def read(
    #     self,
    #     db_engine: str,
    #     db_name: str,
    #     sqlquery: str,
    #     username: str,
    #     password: str,
    #     fetchsize: int = 1000000,
    #     use_proleptic_gregorian_calendar: bool = True,
    # ) -> pyspark.sql.DataFrame:
    #     if use_proleptic_gregorian_calendar:
    #         self.use_proleptic_gregorian_calendar()
    #     match db_engine:
    #         case "mssql" | "mssql-ntlm" | "postgresql":
    #             return (
    #                 self.sparkmanager.session.read.format("jdbc")
    #                 .option(
    #                     "url",
    #                     self.jdbcmanager.get_connection_string(db_engine, db_name),
    #                 )
    #                 .option("driver", self.jdbcmanager.driver)
    #                 .option("fetchsize", fetchsize)
    #                 .option("user", username)
    #                 .option("password", password)
    #                 .option("query", sqlquery)
    #                 .load()
    #             )
    #         case "oracle":
    #             return (
    #                 self.sparkmanager.session.read.format("jdbc")
    #                 .option(
    #                     "url",
    #                     self.jdbcmanager.get_connection_string(db_engine, db_name),
    #                 )
    #                 .option("driver", self.jdbcmanager.driver)
    #                 .option("fetchsize", fetchsize)
    #                 .option("user", username)
    #                 .option("password", password)
    #                 .option("dbtable", f"({sqlquery})")
    #                 .load()
    #             )
    #         case "mysql":
    #             return (
    #                 self.sparkmanager.session.read.format("jdbc")
    #                 .option(
    #                     "url",
    #                     self.jdbcmanager.get_connection_string(db_engine, db_name),
    #                 )
    #                 .option("driver", self.jdbcmanager.driver)
    #                 .option("fetchsize", fetchsize)
    #                 .option("user", username)
    #                 .option("password", password)
    #                 .option("dbtable", f"({sqlquery}) foo")
    #                 .load()
    #             )
    #         case _:
    #             raise ValueError(f"{db_engine}: Database Engine is not Implemented!")

    def read(
        self,
        db_engine: str,
        db_name: str,
        sqlquery: str,
        username: str,
        password: str,
        fetchsize: int = 1000000,
        use_proleptic_gregorian_calendar: bool = True,
    ) -> pyspark.sql.DataFrame:
        if use_proleptic_gregorian_calendar:
            self.use_proleptic_gregorian_calendar()
        if db_engine in ("postgresql", "mssql"):
            return (
                self.sparkmanager.session.read.format("jdbc")
                .option(
                    "url",
                    self.jdbcmanager.get_connection_string(db_engine, db_name),
                )
                .option("driver", self.jdbcmanager.driver)
                .option("fetchsize", fetchsize)
                .option("user", username)
                .option("password", password)
                .option("query", sqlquery)
                .load()
            )
        if db_engine == "oracle":
            return (
                self.sparkmanager.session.read.format("jdbc")
                .option(
                    "url",
                    self.jdbcmanager.get_connection_string(db_engine, db_name),
                )
                .option("driver", self.jdbcmanager.driver)
                .option("fetchsize", fetchsize)
                .option("user", username)
                .option("password", password)
                .option("dbtable", f"({sqlquery})")
                .load()
            )
        if db_engine == "mysql":
            return (
                self.sparkmanager.session.read.format("jdbc")
                .option(
                    "url",
                    self.jdbcmanager.get_connection_string(db_engine, db_name),
                )
                .option("driver", self.jdbcmanager.driver)
                .option("fetchsize", fetchsize)
                .option("user", username)
                .option("password", password)
                .option("dbtable", f"({sqlquery}) foo")
                .load()
            )
        raise ValueError(f"{db_engine}: Database Engine is not Implemented!")
