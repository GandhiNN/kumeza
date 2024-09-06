# pylint: disable=attribute-defined-outside-init

import logging

import pyarrow as pa

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

    def ingest_schema(self, db_name, sql_schema):
        # ingest schema from source
        rs_schema: list = self.extractor.read(
            db_name,
            sql_schema,
            self.pipeline.domain,
            self.pipeline.username,
            self.pipeline.password,
        )
        arrow_rs_schema: pa.lib.Table = self.arrow_converter.from_python_list(rs_schema)
        # Get schema of the table
        # Convert Arrow schema to Hive
        schema: list[dict] = ArrowManager.get_schema(arrow_rs_schema, hive=True)
        return schema

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

    def ingest_schema_sequential_wrapper(self, schema_sink_id, schema_metadata_sink_id):

        # Setup metadata attributes
        self.pipeline.setup_schema_metadata_attributes(
            schema_sink_id, schema_metadata_sink_id
        )

        # loop through ingestion object
        for obj in self.pipeline.ingestion_objects:
            obj["initial_load_flag"] = False
            object_name = obj["table_name"].lower()
            db_name = obj["db_name"]
            sql_schema = obj["sql_statement_schema"]

            # ingest schema from source
            schema = self.ingest_schema(db_name, sql_schema)

            # Get schema hash
            current_schema_hash: str = get_schema_hash(schema)
            logger.info("Schema hash is %s", current_schema_hash)

            last_ing_status = self.pipeline.get_last_ingestion_status(object_name)
            if len(last_ing_status["Items"]) == 0:
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
                prev_schema_hash = last_ing_status["Items"][0]["schema_hash"]["S"]
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

            self.pipeline.ingestion_objects_furnished.append(obj)

    def ingest_raw_data_sequential_wrapper(
        self, raw_data_sink_id, raw_data_metadata_sink_id
    ):

        # Setup metadata attributes
        self.pipeline.setup_raw_data_metadata_attributes(
            raw_data_sink_id, raw_data_metadata_sink_id
        )

        # loop through ingestion object
        for obj in self.pipeline.ingestion_objects_furnished:
            object_name = obj["table_name"].lower()
            db_name = obj["db_name"]
            sql_schema = obj["sql_statement_schema"]
            il_flag = obj["initial_load_flag"]
            print(object_name, db_name, sql_schema, il_flag)

        #     # ingest schema from source
        #     schema = self.ingest_schema(db_name, sql_schema)

        #     # Get schema hash
        #     current_schema_hash: str = get_schema_hash(schema)
        #     logger.info("Schema hash is %s", current_schema_hash)

        #     last_ing_status = self.pipeline.get_last_ingestion_status(object_name)
        #     if len(last_ing_status["Items"]) == 0:
        #         logger.info("Object: %s has never been ingested", object_name)
        #         obj["initial_load_flag"] = True
        #         self.write_schema_to_s3(schema, object_name)
        #         self.register_schema_to_metadata(
        #             object_name, schema, current_schema_hash
        #         )

        #     else:
        #         logger.info("Object %s has been ingested before", object_name)
        #         # compare schema hash
        #         logger.info("Comparing schema hash for object: %s", object_name)
        #         prev_schema_hash = last_ing_status["Items"][0]["schema_hash"]["S"]
        #         if current_schema_hash != prev_schema_hash:
        #             logger.info(
        #                 "Table: %s structure has changed",
        #                 object_name,
        #             )
        #             obj["initial_load_flag"] = True
        #             self.write_schema_to_s3(schema, object_name)
        #         else:
        #             logger.info(
        #                 "Table: %s structure has not changed, continuing...",
        #                 object_name,
        #             )

        #     ingestion_objects_new.append(obj)

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
        self.pipeline.setup_ingestion_objects()
        logger.info(
            "Processing %s numbers of ingestion objects",
            len(self.pipeline.ingestion_objects),
        )
        if concurrent is False:
            self.ingest_schema_sequential_wrapper(
                schema_sink_id, schema_metadata_sink_id
            )
            self.ingest_raw_data_sequential_wrapper(
                raw_data_sink_id, raw_data_metadata_sink_id
            )
        print("Done")
