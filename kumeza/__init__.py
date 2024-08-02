from enum import Enum
import importlib.resources
# import pathlib
import importlib

PARENT = importlib.resources.path(__package__, 'shared_lib')

# PARENT = pathlib.Path(__file__).parent
# PARENT = resources.files(pkg_name)

# class StaticFiles(Enum):
#     PKI_CERT = PARENT / "shared_lib/cert/pki_bundle.crt"
#     JTDS_JAR = PARENT / "shared_lib/jars/jtds-1.3.1.jar"
#     ORACLE_JAR = PARENT / "shared_lib/jars/ojdbc8.jar"
#     MYSQL_JAR = PARENT / "shared_lib/jars/mysql-connector-java-8.0.31.jar"
#     ODBC_CONFIG = PARENT / "shared_lib/odbc/odbcinst.ini"
#     ODBC_TDS_EXTENSION = PARENT / "shared_lib/odbc/extensions/libtdsodbc.so"


# class StaticFiles(Enum):
#     PKI_CERT = PARENT / "shared_lib/cert/pki_bundle.crt"
#     JTDS_JAR = PARENT / "shared_lib/jars/jtds-1.3.1.jar"
#     ORACLE_JAR = PARENT / "shared_lib/jars/ojdbc8.jar"
#     MYSQL_JAR = PARENT / "shared_lib/jars/mysql-connector-java-8.0.31.jar"
#     ODBC_CONFIG = PARENT / "shared_lib/odbc/odbcinst.ini"
#     ODBC_TDS_EXTENSION = PARENT / "shared_lib/odbc/extensions/libtdsodbc.so"

class StaticFiles(Enum):
    PKI_CERT = PARENT / "cert/pki_bundle.crt"
    JTDS_JAR = PARENT / "jars/jtds-1.3.1.jar"
    ORACLE_JAR = PARENT / "jars/ojdbc8.jar"
    MYSQL_JAR = PARENT / "jars/mysql-connector-java-8.0.31.jar"
    ODBC_CONFIG = PARENT / "odbc/odbcinst.ini"
    ODBC_TDS_EXTENSION = PARENT / "odbc/extensions/libtdsodbc.so"
