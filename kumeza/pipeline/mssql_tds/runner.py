# pylint: disable=attribute-defined-outside-init

import logging

from kumeza.connectors.tds import TDSManager
from kumeza.core.arrow import ArrowConverter, ArrowManager
from kumeza.core.data import get_schema_hash
from kumeza.extractors.mssql import MSSQLExtractor
from kumeza.pipeline.mssql_tds.pipeline import Pipeline


logger = logging.getLogger(__name__)


class Runner:

    def __init__(self, hostname, port, db_instance):
        self.tds_manager = TDSManager(hostname, port, db_instance)
        self.extractor: MSSQLExtractor = MSSQLExtractor(self.tds_manager)
        self.arrow_converter: ArrowConverter = ArrowConverter()

    def ingest_schema(self, db_name, sql):
        # ingest schema from source
        logger.info("Executing query: %s", sql)
        rs_schema: list = self.extractor.read(
            db_name,
            sql,
            self.pipeline.domain,
            self.pipeline.username,
            self.pipeline.password,
        )
        return self.arrow_converter.from_python_list(rs_schema)

    def write_schema_to_s3(self, schema, object_name):

        schema_key = (
            f"""{self.pipeline.source_system_id}/{self.pipeline.source_system_physical_location}/"""
            f"""{object_name}/{object_name}-{self.pipeline.dateobj.get_current_timestamp(ts_format="date_only")}.json"""
        )
        self.pipeline.s3.write_to_bucket(
            content=schema, bucket_name=self.pipeline.schema_bucket, key_name=schema_key
        )

    def register_schema_to_metadata(
        self, object_name, object_schema, object_schema_hash
    ):
        partition_key_value = (
            f"""{self.pipeline.source_system_id}-"""
            f"""{self.pipeline.source_system_physical_location}-{object_name}"""
        )
        item = {
            f"{self.pipeline.schema_metadata_table_partition_key}": partition_key_value,
            f"{self.pipeline.schema_metadata_table_sort_key}": self.pipeline.dateobj.get_current_timestamp(
                ts_format="epoch"
            ),
            "schema": object_schema,
            "schema_hash": object_schema_hash,
        }
        logger.info("Registering schema of table: %s => %s", object_name, item)
        logger.info(
            self.pipeline.dynamodb.put_item(item, self.pipeline.schema_metadata_table)
        )

    def get_row_count_from_query(self, db_name, sql):
        logger.info("Executing query: %s", sql)
        rs = self.extractor.read(
            db_name,
            sql,
            self.pipeline.domain,
            self.pipeline.username,
            self.pipeline.password,
        )
        return rs

    def ingest_raw_data(self, db_name, sql):

        logger.info("Executing query: %s", sql)
        rs_raw: list = self.extractor.read(
            db_name,
            sql,
            self.pipeline.domain,
            self.pipeline.username,
            self.pipeline.password,
        )
        arrow_result_sets_raw = self.arrow_converter.from_python_list(rs_raw)
        return arrow_result_sets_raw

    def get_row_count_from_result_set(self, result_set):
        return result_set.num_rows

    def write_raw_data_to_s3(self, result_set, object_name):

        # Write raw to raw bucket
        raw_key = (
            f"""{self.pipeline.source_system_id}/{self.pipeline.source_system_physical_location}/"""
            f"""{object_name}/{self.pipeline.dateobj.get_current_timestamp(ts_format="date_only")}/"""
        )
        ArrowManager.write_to_s3(
            result_set, f"s3://{self.pipeline.raw_data_bucket}/{raw_key}"
        )

    def register_ingestion_status_to_metadata(self, object_name, row_count):
        partition_key_value = (
            f"""{self.pipeline.source_system_id}-"""
            f"""{self.pipeline.source_system_physical_location}-{object_name}"""
        )
        item = {
            f"{self.pipeline.raw_data_metadata_table_partition_key}": partition_key_value,
            f"{self.pipeline.raw_data_metadata_table_sort_key}": self.pipeline.dateobj.get_current_timestamp(
                ts_format="epoch"
            ),
            "table": object_name,
            "row_count": row_count,
        }
        logger.info(
            "Registering ingestion status of table: %s => %s", object_name, item
        )
        logger.info(
            self.pipeline.dynamodb.put_item(item, self.pipeline.raw_data_metadata_table)
        )

    def ingest_schema_sequential_wrapper(
        self, ingestion_objects, schema_sink_id, schema_metadata_sink_id
    ) -> list:

        # Setup metadata attributes
        self.pipeline.setup_schema_metadata_attributes(
            schema_sink_id, schema_metadata_sink_id
        )

        # Setup furnished ingestion objects for raw data ingestion phase
        ingestion_objects_raw = []

        # loop through ingestion object
        for obj in ingestion_objects:
            obj["initial_load_flag"] = False
            object_name = obj["table_name"].lower()
            db_name = obj["db_name"]
            sql = obj["sql_statement_schema"]

            # ingest schema from source
            rs = self.ingest_schema(db_name, sql)
            rc = self.get_row_count_from_result_set(rs)
            if rc > 0:
                # Get schema of the table
                # Convert Arrow schema to Hive
                schema: list[dict] = ArrowManager.get_schema(rs, hive=True)

                # Get schema hash
                current_schema_hash: str = get_schema_hash(schema)
                logger.info("Schema hash is %s", current_schema_hash)

                # Get the latest schema registered in metadata table
                last_schema = self.pipeline.get_last_ingestion_status(
                    self.pipeline.schema_metadata_table,
                    self.pipeline.schema_metadata_table_partition_key,
                    self.pipeline.schema_metadata_table_sort_key,
                    object_name,
                )
                if len(last_schema["Items"]) == 0:
                    logger.info("Object: %s has never been ingested", object_name)
                    obj["initial_load_flag"] = True
                    self.write_schema_to_s3(schema, object_name)
                    self.register_schema_to_metadata(
                        object_name, schema, current_schema_hash
                    )
                else:
                    logger.info("Object %s has been ingested before", object_name)
                    # compare schema hash
                    logger.info("Comparing schema hash for object: %s", object_name)
                    prev_schema_hash = last_schema["Items"][0]["schema_hash"]["S"]
                    if current_schema_hash != prev_schema_hash:
                        logger.info(
                            "Table: %s structure has changed",
                            object_name,
                        )
                        obj["initial_load_flag"] = True
                        self.write_schema_to_s3(schema, object_name)
                        self.register_schema_to_metadata(
                            object_name, schema, current_schema_hash
                        )
                    else:
                        logger.info(
                            "Table: %s structure has not changed, continuing...",
                            object_name,
                        )
                ingestion_objects_raw.append(obj)
            else:
                logger.info("Table: %s contains 0 records! Skipping...")
                continue
        return ingestion_objects_raw

    def ingest_raw_data_sequential_wrapper(
        self, ingestion_objects_raw, raw_data_sink_id, raw_data_metadata_sink_id
    ):
        # Setup metadata attributes
        self.pipeline.setup_raw_data_metadata_attributes(
            raw_data_sink_id, raw_data_metadata_sink_id
        )

        # loop through ingestion object
        for obj in ingestion_objects_raw:
            object_name = obj["table_name"].lower()
            db_name = obj["db_name"]
            sql = obj["sql_statement_raw"]
            sql_row_count = obj["sql_row_count"]
            il_flag = obj["initial_load_flag"]

            # ingest raw data
            # 1. if il_flag is set to true, then skip checking the last ingestion status
            # because it will be considered as the first ingestion anyway
            if il_flag:
                rc = self.get_row_count_from_query(
                    db_name, sql_row_count
                )  # output: [{'rowCount': <rowcount_int>}]
                print(rc, type(rc))
                if rc[0]["rowCount"] > 0:
                    # check last ingestion status of the table and determine
                    last_ing_status = self.pipeline.get_last_ingestion_status(
                        self.pipeline.raw_data_metadata_table,
                        self.pipeline.raw_data_metadata_table_partition_key,
                        self.pipeline.raw_data_metadata_table_sort_key,
                        object_name,
                    )
                    if len(last_ing_status["Items"]) == 0:
                        logger.info("Object: %s has never been ingested", object_name)
                        rs = self.ingest_raw_data(db_name, sql)
                        self.write_raw_data_to_s3(rs, object_name)
                        self.register_ingestion_status_to_metadata(object_name, rc)
                else:
                    logger.info(
                        "Query to table: %s generates 0 records! Skipping...",
                        object_name,
                    )
                    continue
            else:
                logger.info("Executing delta load logic for table: %s", object_name)

    def run(
        self,
        ingestion_config,
        credentials,
        schema_sink_id,
        schema_metadata_sink_id,
        raw_data_sink_id,
        raw_data_metadata_sink_id,
        concurrent=False,
    ):
        # Setup basic pipeline attributes
        self.pipeline = Pipeline(ingestion_config, credentials)
        self.pipeline.setup()
        ingestion_objects = self.pipeline.setup_ingestion_objects()
        logger.info(
            "Processing %s numbers of ingestion objects",
            len(ingestion_objects),
        )
        if concurrent is False:
            ingestion_objects_raw = self.ingest_schema_sequential_wrapper(
                ingestion_objects, schema_sink_id, schema_metadata_sink_id
            )
            self.ingest_raw_data_sequential_wrapper(
                ingestion_objects_raw, raw_data_sink_id, raw_data_metadata_sink_id
            )
        logger.info("Ingestion finished!")
