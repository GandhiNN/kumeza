import pymssql


class TDSExtractor:

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
        return cursor.fetchall()
