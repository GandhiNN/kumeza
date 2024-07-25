import unittest

from kumeza.connectors.spark import JDBCManager


# Constant definition
HOSTNAME = "somedbhostname.db.sdi.pmi"
PORT = "1443"
DB_INSTANCE = "dev"
DB_NAME = "somedbname"
DOMAIN = "somedomain.net"


class JDBCManagerTest(unittest.TestCase):

    def setUp(self):
        self.hostname = HOSTNAME
        self.port = PORT
        self.db_instance = DB_INSTANCE
        self.db_name = DB_NAME
        self.domain = DOMAIN
        self.jdbc_manager = JDBCManager(
            self.hostname, self.port, self.db_instance, self.db_name, self.domain
        )

    def test_get_connstring_mssql(self):
        assert (
            self.jdbc_manager.get_connection_string("mssql")
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=false"""
        )
        assert (
            self.jdbc_manager.get_connection_string("mssql-ntlm")
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=true;useNTLMv2=true;domain=somedomain.net"""
        )

    def test_get_connstring_postgresql(self):
        assert (
            self.jdbc_manager.get_connection_string("postgresql")
            == """jdbc:jtds:sqlserver://somedbhostname.db.sdi.pmi:1443;"""
            """instance=dev;databaseName=somedbname;"""
            """integratedSecurity=true;useNTLMv2=true;domain=somedomain.net"""
        )

    def test_get_connstring_oracle(self):
        assert (
            self.jdbc_manager.get_connection_string("oracle")
            == """jdbc:oracle:thin:@somedbhostname.db.sdi.pmi:1443:dev"""
        )

    def test_get_connstring_mysql(self):
        assert (
            self.jdbc_manager.get_connection_string("mysql")
            == """jdbc:mysql://somedbhostname.db.sdi.pmi:1443/somedbname"""
            """?zeroDateTimeBehavior=CONVERT_TO_NULL&autoCommit=false"""
        )
    
    def test_get_connstring_unrecognized_input(self):
        assert (
            self.jdbc_manager.get_connection_string("unrecognized") == ""
        )

    def test_get_driver_mssql(self):
        assert (
            self.jdbc_manager.get_driver("mssql") == "net.sourceforge.jtds.jdbc.Driver"
        )
        assert (
            self.jdbc_manager.get_driver("mssql-ntlm")
            == "net.sourceforge.jtds.jdbc.Driver"
        )

    def test_get_driver_postgresql(self):
        assert (
            self.jdbc_manager.get_driver("postgresql")
            == "net.sourceforge.jtds.jdbc.Driver"
        )

    def test_get_driver_oracle(self):
        assert (
            self.jdbc_manager.get_driver("oracle") == "oracle.jdbc.driver.OracleDriver"
        )

    def test_get_driver_mysql(self):
        assert self.jdbc_manager.get_driver("mysql") == "com.mysql.cj.jdbc.Driver"

    def test_get_driver_unrecognized_input(self):
        assert self.jdbc_manager.get_driver("unrecognized") == ""
