# pylint: disable=attribute-defined-outside-init
import logging
import sys
from typing import Any, Tuple

import pymssql

from kumeza.connectors.tds import TDSManager


logger = logging.getLogger(__name__)


class Extractor:
    def __init__(self, tdsmanager: TDSManager):
        self.tdsmanager = tdsmanager

    def _return_dict_pair(
        self, cursor: pymssql.Cursor, row_item: list[Tuple[str, Any]]
    ) -> dict:
        return_dict = {}
        for column_name, row in zip(cursor.description, row_item):
            return_dict[column_name[0]] = row
        return return_dict

    def read(
        self,
        db_name: str,
        sqlquery: str,
        domain: str,
        username: str,
        password: str,
    ) -> Tuple[list[dict[str, Any]], int]:
        try:
            conn: pymssql.Connection = None
            logger.info("Connecting to the database")
            logger.info("Using authentication type: %s", self.tdsmanager.auth)
            if self.tdsmanager.auth != "windows_authentication":
                conn = pymssql.connect(
                    server=self.tdsmanager.get_connection_string(),  # pragma: allowlist-secret
                    user=f"{domain}\\{username}",
                    password=password,  # pragma: allowlist-secret
                    database=db_name,
                )
            else:
                # turn f-string into raw string to handle backslash / special chars
                host = rf"{self.tdsmanager.get_connection_string()}"
                port = rf"{self.tdsmanager.port}"
                user = rf"{domain}\{username}"
                password = rf"{password}"
                conn = pymssql.connect(
                    host=host, port=port, user=user, password=password
                )
            cursor = conn.cursor()
            logger.info("Executing query: %s", sqlquery)
            cursor.execute(sqlquery)

            return_list = []
            for row in cursor:
                row_item = self._return_dict_pair(cursor, row)
                return_list.append(row_item)

            return return_list, sys.getsizeof(return_list)
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            # Make sure to close the connection
            logger.info("Closing the connection to the database")
            conn.close()
