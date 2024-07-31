from kumeza.connectors.spark import SparkManager

class SparkExtractor:

    def __init__(self, sparkmanager: SparkManager):
        self.sparkmanager = sparkmanager

    def set_spark_debug_level(self, level: str = "DEBUG"):
        self.sparkmanager.SparkContext.setLogLevel(level)

    def use_proleptic_gregorian_calendar(self):
        """# https://issues.apache.org/jira/browse/SPARK-31408"""
        pass

    def read(self):
        pass