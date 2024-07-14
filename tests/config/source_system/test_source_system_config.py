import os
import unittest

from kumeza.config.loader import ConfigLoader
from kumeza.config.source_system.source_system_config import SourceSystemConfig


# Config files to be referenced
ABS_PATH = os.path.dirname(__file__)
JSON_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.json")
YAML_CONFIG = os.path.join(ABS_PATH, "..", "files", "config.yaml")


class TestConfigInstanceSourceSystem:
    def __init__(self):
        self.source_system = SourceSystemConfig(
            id="imel",
            database_type="mssql",
            database_instance="dev",
            authentication_type="ntlm",
            hostname="sqlqa_qimel_pmhboz.dbiaas.sdi.pmi",
            domain="pmintl.net",
            port=1433,
        )


class TestSetUp:  # pragma: no cover
    def __init__(self):
        self.json_config = None
        self.yml_config = None

    def setup(self) -> None:
        self.json_config = ConfigLoader.load(JSON_CONFIG)
        self.yml_config = ConfigLoader.load(YAML_CONFIG)


class TestSourceSystemConfig(unittest.TestCase, TestSetUp):
    def setUp(self) -> None:
        self.setup()

    def test(self):
        base_config = TestConfigInstanceSourceSystem()
        expected = base_config.source_system
        self.assertEqual(
            SourceSystemConfig.marshal(self.yml_config["source_system"]),
            expected,
        )
        self.assertEqual(
            SourceSystemConfig.marshal(self.json_config["source_system"]),
            expected,
        )


def testSuite():  # pragma: no cover
    suite = unittest.TestSuite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(
            TestSourceSystemConfig,
        )
    )  # pragma: no cover


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(testSuite())  # pragma: no cover
