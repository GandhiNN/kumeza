# pylint: disable=attribute-defined-outside-init

import unittest

import pytest

from kumeza.config.data_assets.data_assets_config import Assets, AssetsId
from kumeza.config.source_system.source_system_config import SourceSystemConfig
from kumeza.utils.query.mssql import MSSQLQueryManager, MSSQLQueryTemplater


# raise unittest.SkipTest("##TODO")

# Constant
src_sys_cfg = SourceSystemConfig(
    id="custdb",
    type="rdbms",
    database_engine="mssql",
    database_instance="dev",
    authentication_type="ntlm",
    hostname="custdb.mssql.example",
    domain="example.com",
    port=1433,
    physical_location="jkt",
)

src_sys_cfg_faulty_type = SourceSystemConfig(
    id="custdb",
    type="nosql",
    database_engine="mssql",
    database_instance="dev",
    authentication_type="ntlm",
    hostname="custdb.mssql.example",
    domain="example.com",
    port=1433,
    physical_location="jkt",
)

src_sys_cfg_faulty_db_engine = SourceSystemConfig(
    id="custdb",
    type="rdbms",
    database_engine="snowflake",
    database_instance="dev",
    authentication_type="ntlm",
    hostname="custdb.mssql.example",
    domain="example.com",
    port=1433,
    physical_location="jkt",
)

data_assets = AssetsId(
    id="group_1",
    database_name="master",
    assets=[
        Assets(
            asset_name="CUSTOMER_ID",
            asset_type="table",
            database_schema="dbo",
            query_type="standard",
            incremental=True,
            incremental_column="updated_at",
            reload=False,
            partition_columns=[],
            columns_to_anonymize=[],
            custom_query="SELECT col1, col2 FROM master.dbo.CUSTOMER_ID where col3 = 'testVal'",
            custom_schema={},
            cast_timestamp_columns_to_string=False,
        )
    ],
)

# assets = Assets(
#     asset_name="CUSTOMER_ID",
#     asset_type="table",
#     database_schema="dbo",
#     query_type="standard",
#     incremental=True,
#     incremental_column="updated_at",
#     reload=False,
#     partition_columns=[],
#     columns_to_anonymize=[],
#     custom_query="SELECT col1, col2 FROM master.dbo.CUSTOMER_ID where col3 = 'testVal'",
#     custom_schema={},
#     cast_timestamp_columns_to_string=False,
# )


class MSSQLQueryManagerTest(unittest.TestCase):

    def test_fail_to_init_mssql_query_manager_due_to_faulty_type(self):
        with pytest.raises(TypeError):
            self.mssql_query_manager_faulty = MSSQLQueryManager(src_sys_cfg_faulty_type)

    def test_fail_to_init_mssql_query_manager_due_to_faulty_db_engine(self):
        with pytest.raises(ValueError):
            self.mssql_query_manager_faulty = MSSQLQueryManager(
                src_sys_cfg_faulty_db_engine
            )


class MSSQLQueryTemplaterTest(unittest.TestCase):

    def setUp(self):
        self.mssql_query_templater = MSSQLQueryTemplater(src_sys_cfg, data_assets)

    def test_generate_schema_query(self):
        expected = "SELECT TOP 1000 * FROM master.dbo.CUSTOMER_ID"
        assert self.mssql_query_templater.get_sql_query(mode="schema") == expected

    def test_generate_custom_query(self):
        expected = (
            "SELECT col1, col2 FROM master.dbo.CUSTOMER_ID where col3 = 'testVal'"
        )
        assert self.mssql_query_templater.get_sql_query(mode="custom") == expected

    def test_generate_standard_query(self):
        expected = "SELECT * FROM master.dbo.CUSTOMER_ID"
        assert self.mssql_query_templater.get_sql_query(mode="standard") == expected

    def test_generate_incremental_query(self):
        expected = """SELECT * FROM master.dbo.CUSTOMER_ID WHERE updated_at >= '2024-09-07 10:00:00' AND updated_at <= '2024-09-07 12:00:00'"""
        assert (
            self.mssql_query_templater.get_sql_query(
                mode="incremental",
                start_time="2024-09-07 10:00:00",
                end_time="2024-09-07 12:00:00",
            )
            == expected
        )

    def test_fail_to_generate_custom_query_due_to_faulty_mode(self):
        with pytest.raises(ValueError):
            self.mssql_query_templater.get_sql_query(mode="aggregation")
