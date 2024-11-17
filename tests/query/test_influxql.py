# pylint: disable=attribute-defined-outside-init

import unittest

import pytest

from kumeza.config.data_assets.data_assets_config import Assets, AssetsId
from kumeza.config.source_system.source_system_config import SourceSystemConfig
from kumeza.query.influxql import InfluxQLQueryManager, InfluxQLQueryTemplater


# raise unittest.SkipTest("##TODO")

# Constant
src_sys_cfg = SourceSystemConfig(
    id="historian",
    type="timeseries",
    database_engine="influx",
    database_instance="dev",
    authentication_type="simple",
    hostname="https://custdb.mssql.example/query?",
    domain="example.com",
    port=443,
    physical_location="jkt",
)

src_sys_cfg_faulty_type = SourceSystemConfig(
    id="historian",
    type="nosql",
    database_engine="influx",
    database_instance="dev",
    authentication_type="simple",
    hostname="https://custdb.mssql.example/query?",
    domain="example.com",
    port=443,
    physical_location="jkt",
)

src_sys_cfg_faulty_db_engine = SourceSystemConfig(
    id="historian",
    type="timeseries",
    database_engine="snowflake",
    database_instance="dev",
    authentication_type="simple",
    hostname="https://custdb.mssql.example/query?",
    domain="example.com",
    port=443,
    physical_location="jkt",
)

data_assets = AssetsId(
    id="group_1",
    database_name="historian",
    assets=[
        Assets(
            asset_name="HVAC",
            asset_type="table",
            database_schema="",
            query_type="custom",
            incremental=True,
            incremental_column="Timestamp",
            reload=False,
            partition_columns=[],
            columns_to_anonymize=[],
            custom_query=" ".join(
                (
                    "select id as MeterName,",
                    "time as Timestamp, value as Value from",
                    "Gemt.136-59-L4-CA-CPA_13.PVValue.59-L4-CA-CPA_13",
                    "where topic =~ /GEMT/ and time > now() - 15m",
                    "order by time desc",
                )
            ),
            custom_schema={},
            cast_timestamp_columns_to_string=False,
        )
    ],
)

CUSTOM_QUERY = data_assets.assets[0].custom_query
DATABASE_NAME = data_assets.database_name
DATABASE_SCHEMA = data_assets.assets[0].database_schema
OBJECT_NAME = data_assets.assets[0].asset_name
INCREMENTAL_COLUMN = data_assets.assets[0].incremental_column


class InfluxQLQueryManagerTest(unittest.TestCase):
    def test_fail_to_init_query_manager_due_to_faulty_type(self):
        with pytest.raises(TypeError):
            self.query_manager_faulty = InfluxQLQueryManager(src_sys_cfg_faulty_type)

    def test_fail_to_init_query_manager_due_to_faulty_db_engine(self):
        with pytest.raises(ValueError):
            self.query_manager_faulty = InfluxQLQueryManager(
                src_sys_cfg_faulty_db_engine
            )


class InfluxQLQueryTemplaterTest(unittest.TestCase):
    def setUp(self):
        self.query_templater = InfluxQLQueryTemplater(src_sys_cfg)

    def test_generate_custom_query(self):
        expected = "select id as MeterName, time as Timestamp, value as Value from Gemt.136-59-L4-CA-CPA_13.PVValue.59-L4-CA-CPA_13 where topic =~ /GEMT/ and time > now() - 15m order by time desc"
        assert (
            self.query_templater.get_query(mode="custom", custom_query=CUSTOM_QUERY)
            == expected
        )

    def test_fail_to_generate_custom_query_due_to_faulty_mode(self):
        with pytest.raises(ValueError):
            self.query_templater.get_query(mode="aggregation")
