from typing import Any, Tuple

import pymssql


class MSSQLExtractor:

    def _return_dict_pair(
        self, cursor: pymssql.Cursor, row_item: list[Tuple[str, Any]]
    ) -> dict:
        return_dict = {}
        for column_name, row in zip(cursor.description, row_item):
            return_dict[column_name[0]] = row
        return return_dict

    def read(
        self,
        hostname: str,
        port: str,
        db_name: str,
        db_instance: str,
        sqlquery: str,
        domain: str,
        username: str,
        password: str,
    ) -> list[dict[str, Any]]:
        try:
            conn = pymssql.connect(
                server=f"{hostname}:{port}\\{db_instance}",
                user=f"{domain}\\{username}",
                password=password,
                database=db_name,
            )
            cursor = conn.cursor()
            cursor.execute(sqlquery)

            return_list = []
            for row in cursor:
                row_item = self._return_dict_pair(cursor, row)
                return_list.append(row_item)

            conn.close()
            return return_list
        except Exception as e:
            raise e
