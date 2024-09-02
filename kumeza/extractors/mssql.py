import logging
from typing import Any, Tuple

import pymssql

from kumeza.connectors.tds import TDSManager


logger = logging.getLogger(__name__)


class MSSQLExtractor:

    def __init__(
        self,
        tdsmanager: TDSManager,
        domain: str,
        username: str,
        password: str,
        db_name: str,
    ):
        self.tdsmanager = tdsmanager
        self.conn = pymssql.connect(
            server=self.tdsmanager.get_connection_string(),
            user=f"{domain}\\{username}",
            password=password,
            database=db_name,
        )

    def _return_dict_pair(
        self, cursor: pymssql.Cursor, row_item: list[Tuple[str, Any]]
    ) -> dict:
        return_dict = {}
        for column_name, row in zip(cursor.description, row_item):
            return_dict[column_name[0]] = row
        return return_dict

    def close_connection(self):
        self.conn.close()

    def read(
        self,
        sqlquery: str,
    ) -> list[dict[str, Any]]:
        try:
            logger.info("Connecting to the database")
            # conn = pymssql.connect(
            #     server=self.tdsmanager.get_connection_string(),
            #     user=f"{domain}\\{username}",
            #     password=password,
            #     database=db_name,
            # )
            cursor = self.conn.cursor()
            cursor.execute(sqlquery)

            return_list = []
            for row in cursor:
                row_item = self._return_dict_pair(cursor, row)
                return_list.append(row_item)

            return return_list
        except Exception as e:
            logger.error(e)
            raise e
