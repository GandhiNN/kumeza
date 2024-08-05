import unittest

import pytest

from kumeza.connectors.jdbc import JDBCManager


# Constant definition
HOSTNAME = "somedbhostname.db.sdi.pmi"
PORT = "1443"
DB_INSTANCE = "dev"
DB_NAME = "somedbname"
DOMAIN = "somedomain.net"
DB_ENGINE = "mssql"


class JDBCManagerTest(unittest.TestCase):

    def setUp(self):
        self.hostname = HOSTNAME
        self.port = PORT
        self.db_instance = DB_INSTANCE
        self.db_name = DB_NAME
        self.domain = DOMAIN
        self.db_engine = DB_ENGINE
        self.jdbc_manager = JDBCManager(
            self.hostname, self.port, self.db_instance, self.domain, self.db_engine
        )

    def test_get_connstring_mssql(self):
        assert (
            self.jdbc_manager.get_connection_string("mssql", DB_NAME)
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=false"""
        )
        assert (
            self.jdbc_manager.get_connection_string("mssql-ntlm", DB_NAME)
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=true;useNTLMv2=true;domain=somedomain.net"""
        )

    def test_get_connstring_postgresql(self):
        assert (
            self.jdbc_manager.get_connection_string("postgresql", DB_NAME)
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=true;useNTLMv2=true;domain=somedomain.net"""
        )

    def test_get_connstring_oracle(self):
        assert (
            self.jdbc_manager.get_connection_string("oracle", DB_NAME)
            == """jdbc:oracle:thin:@somedbhostname.db.sdi.pmi:1443:dev"""
        )

    def test_get_connstring_mysql(self):
        assert (
            self.jdbc_manager.get_connection_string("mysql", DB_NAME)
            == """jdbc:mysql://somedbhostname.db.sdi.pmi:1443/somedbname"""
            """?zeroDateTimeBehavior=CONVERT_TO_NULL&autoCommit=false"""
        )

    def test_get_connstring_unrecognized_input(self):
        with pytest.raises(ValueError):
            self.jdbc_manager.get_connection_string("unrecognized", DB_NAME)

    def test_get_driver_mssql(self):
        assert (
            self.jdbc_manager._get_driver("mssql") == "net.sourceforge.jtds.jdbc.Driver"
        )
        assert (
            self.jdbc_manager._get_driver("mssql-ntlm")
            == "net.sourceforge.jtds.jdbc.Driver"
        )

    def test_get_driver_postgresql(self):
        assert (
            self.jdbc_manager._get_driver("postgresql")
            == "net.sourceforge.jtds.jdbc.Driver"
        )

    def test_get_driver_oracle(self):
        assert (
            self.jdbc_manager._get_driver("oracle") == "oracle.jdbc.driver.OracleDriver"
        )

    def test_get_driver_mysql(self):
        assert self.jdbc_manager._get_driver("mysql") == "com.mysql.cj.jdbc.Driver"

    def test_get_driver_unrecognized_input(self):
        with pytest.raises(ValueError):
            self.jdbc_manager._get_driver("unrecognized")
