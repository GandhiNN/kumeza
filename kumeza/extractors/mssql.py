# pylint: disable=attribute-defined-outside-init
import logging
from typing import Any, Tuple

import pymssql

from kumeza.connectors.tds import TDSManager
from kumeza.core.sqlalchemy import Engine


logger = logging.getLogger(__name__)


class MSSQLExtractor(Engine):

    def __init__(self, tdsmanager: TDSManager):
        super().__init__(dialect="mssql", driver="pymssql")
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
    ) -> list[dict[str, Any]]:
        try:
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
            cursor.execute(sqlquery)

            return_list = []
            for row in cursor:
                row_item = self._return_dict_pair(cursor, row)
                return_list.append(row_item)

            return return_list
        except Exception as e:
            logger.error(e)
            raise e

    def read_using_sqlalchemy(
        self,
        db_name: str,
        sqlquery: str,
        domain: str,
        host: str,
        port: int,
        username: str,
        password: str,
    ):
        engine = self.create_engine(
            domain, username, password, host, port, db_name  # pragma: allowlist-secret
        )
        print(engine, sqlquery)
