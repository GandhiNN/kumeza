import pkgutil
from enum import Enum


class StaticFiles(Enum):
    PKI_CERT = pkgutil.get_data(__name__, "shared_lib/cert/pki_bundle.crt")
    JTDS_JAR = pkgutil.get_data(__name__, "shared_lib/jars/jtds-1.3.1.jar")
    ORACLE_JAR = pkgutil.get_data(__name__, "shared_lib/jars/ojdbc8.jar")
    MYSQL_JAR = pkgutil.get_data(
        __name__, "shared_lib/jars/mysql-connector-java-8.0.31.jar"
    )
    ODBC_CONFIG = pkgutil.get_data(__name__, "shared_lib/odbc/odbcinst.ini")
    ODBC_TDS_EXTENSION = pkgutil.get_data(
        __name__, "shared_lib/odbc/extensions/libtdsodbc.so"
    )
