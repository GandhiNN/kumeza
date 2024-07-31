from pyspark.sql import SparkSession


class SparkManager:

    def __init__(self, pipeline_name: str):
        self.session = (
            SparkSession.builder.config("spark.logConf", "true")
            .appName(pipeline_name)
            .getOrCreate()
        )

    def stop_session(self):
        self.session.stop()
