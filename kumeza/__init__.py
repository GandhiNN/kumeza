import pathlib
from enum import Enum


PARENT = pathlib.Path(__file__).parent


class StaticFiles(Enum):
    PKI_CERT = PARENT / "shared_lib/cert/pki_bundle.crt"
    JTDS_JAR = PARENT / "shared_lib/jars/jtds-1.3.1.jar"
    ORACLE_JAR = PARENT / "shared_lib/jars/ojdbc8.jar"
    MYSQL_JAR = PARENT / "shared_lib/jars/mysql-connector-java-8.0.31.jar"
    ODBC_CONFIG = PARENT / "shared_lib/odbc/odbcinst.ini"
    ODBC_TDS_EXTENSION = PARENT / "shared_lib/odbc/extensions/libtdsodbc.so"
