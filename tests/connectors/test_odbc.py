import unittest

import pytest

from kumeza.connectors.odbc import ODBCManager


# Constant definition
HOSTNAME = "somedbhostname.db.sdi.pmi"
PORT = "1443"
DB_INSTANCE = "dev"
DB_NAME = "somedbname"
DOMAIN = "somedomain.net"
DB_ENGINE = "mssql"
UID = "someuid"
USERNAME = "someusername"
PASSWORD = "somepassword"


class ODBCManagerTest(unittest.TestCase):

    def setUp(self):
        self.hostname = HOSTNAME
        self.port = PORT
        self.db_instance = DB_INSTANCE
        self.db_name = DB_NAME
        self.domain = DOMAIN
        self.db_engine = DB_ENGINE
        self.odbc_manager = ODBCManager(
            self.hostname, self.port, self.db_instance, self.domain, self.db_engine
        )

    def test_get_connstring_mssql(self):
        assert (
            self.odbc_manager.get_connection_string(
                DB_ENGINE, DB_NAME, UID, USERNAME, PASSWORD
            )
            == """DRIVER=FreeTDS;SERVER=somedbhostname.db.sdi.pmi;PORT=1443;"""
            """DATABASE=somedbname;UID=someuid\\someusername;PWD=somepassword;"""
            """DOMAIN=somedomain.net;IntegratedSecurity=True;"""
            """TrustServerCertificate=Yes;TrustedConnection=No"""
        )

    def test_get_connstring_db_type_not_recognized(self):
        with pytest.raises(ValueError):
            self.odbc_manager.get_connection_string(
                "unknowndb", DB_NAME, UID, USERNAME, PASSWORD
            )

    def test_get_driver_mssql(self):
        assert self.odbc_manager.get_driver("mssql") == "FreeTDS"

    def test_get_driver_db_type_not_recognized(self):
        with pytest.raises(ValueError):
            self.odbc_manager.get_driver("unknowndb")
