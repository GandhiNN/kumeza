# from typing import Any, Union

import pymssql


class TDSExtractor:

    # def read(
    #     self,
    #     hostname: str,
    #     port: str,
    #     db_name: str,
    #     db_instance: str,
    #     sqlquery: str,
    #     domain: str,
    #     username: str,
    #     password: str,
    # ) -> Union[list[tuple[Any, ...]], None]:
    #     conn = pymssql.connect(
    #         server=f"{hostname}:{port}\\{db_instance}",
    #         user=f"{domain}\\{username}",
    #         password=password,
    #         database=db_name,
    #     )
    #     cursor = conn.cursor()
    #     cursor.execute(sqlquery)
    #     return cursor.fetchall()

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
    ):
        conn = pymssql.connect(
            server=f"{hostname}:{port}\\{db_instance}",
            user=f"{domain}\\{username}",
            password=password,
            database=db_name,
        )
        cursor = conn.cursor()
        cursor.execute(sqlquery)

        def _return_dict_pair(row_item):
            return_dict = {}
            for column_name, row in zip(cursor.description, row_item):
                return_dict[column_name[0]] = row
            return return_dict

        return_list = []
        for row in cursor:
            row_item = _return_dict_pair(row)
            return_list.append(row_item)

        conn.close()
        return return_list
