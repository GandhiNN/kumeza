import pkgutil
from enum import Enum


class StaticFiles(Enum):
    pki_cert = pkgutil.get_data(__name__, "shared_lib/cert/pki_bundle.crt")
    jtds_jar = pkgutil.get_data(__name__, "shared_lib/jars/jtds-1.3.1.jar")
    oracle_jar = pkgutil.get_data(__name__, "shared_lib/jars/ojdbc8.jar")
    mysql_jar = pkgutil.get_data(
        __name__, "shared_lib/jars/mysql-connector-java-8.0.31.jar"
    )
    odbc_config = pkgutil.get_data(__name__, "shared_lib/odbc/odbcinst.ini")
    odbc_tds_extension = pkgutil.get_data(
        __name__, "shared_lib/odbc/extensions/libtdsodbc.so"
    )
