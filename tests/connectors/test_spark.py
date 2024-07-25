import unittest

from kumeza.connectors.spark import SparkManager


# Constant definition
HOSTNAME = "somedbhostname.db.sdi.pmi"
PORT = "1443"
DB_INSTANCE = "dev"
DB_NAME = "somedbname"
DOMAIN = "somedomain.net"


class JDBCTest(unittest.TestCase):

    def setUp(self):
        self.hostname = HOSTNAME
        self.port = PORT
        self.db_instance = DB_INSTANCE
        self.db_name = DB_NAME
        self.domain = DOMAIN
        self.spark_manager = SparkManager(
            self.hostname, self.port, self.db_instance, self.db_name, self.domain
        )

    def test_get_connstring_mssql(self):
        assert (
            self.spark_manager.get_connection_string("mssql")
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=false"""
        )
        assert (
            self.spark_manager.get_connection_string("mssql-ntlm")
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=true;useNTLMv2=true;domain=somedomain.net"""
        )

    def test_get_driver_mssql(self):
        assert (
            self.spark_manager.get_driver("mssql") == "net.sourceforge.jtds.jdbc.Driver"
        )
        assert (
            self.spark_manager.get_driver("mssql-ntlm")
            == "net.sourceforge.jtds.jdbc.Driver"
        )

    def test_get_connstring_postgresql(self):
        pass

    def test_get_driver_postgresql(self):
        pass
