import unittest

from pyspark.sql import SparkSession  # noqa: E402

from kumeza.connectors.spark import SparkManager  # noqa: E402


PIPELINE_NAME = "test_pipeline"


class SparkManagerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark_manager = SparkManager()
        cls.spark_manager.start_session(PIPELINE_NAME)

    def test_start_spark_session(self):
        assert isinstance(self.spark_manager.session, SparkSession)

    def test_stop_spark_session(self):
        self.spark_manager.stop_session()
        NoneType = type(None)
        print(type(self.spark_manager.session))
        assert isinstance(self.spark_manager.session._sc._jsc, NoneType)
