import logging

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


class SparkManager:

    def __init__(self, pipeline_name: str):
        self.session = (
            SparkSession.builder.config("spark.logConf", "true")
            .appName(pipeline_name)
            .getOrCreate()
        )

    def stop_session(self):
        logger.info("Stopping Spark session")
        self.session.stop()
