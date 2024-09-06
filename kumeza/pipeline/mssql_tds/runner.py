# pylint: disable=attribute-defined-outside-init

from kumeza.connectors.tds import TDSManager
from kumeza.extractors.mssql import MSSQLExtractor
from kumeza.pipeline.mssql_tds.pipeline import Pipeline


class Runner:

    def __init__(self, hostname, port, db_instance):
        self.tds_manager = TDSManager(hostname, port, db_instance)
        self.extractor: MSSQLExtractor = MSSQLExtractor(self.tds_manager)


    def ingestion_wrapper(
        self, ingestion_object, schema_sink_id, schema_metadata_sink_id
    ):
        # Variable expansion
        object_name = ingestion_object["table_name"].lower()
        db_name = ingestion_object["db_name"]
        sql_schema = ingestion_object["sql_statement_schema"]
        print(object_name, db_name, sql_schema)

        # Setup metadata attributes
        self.pipeline.setup_metadata_attributes(schema_sink_id, schema_metadata_sink_id)

        # self.pipeline.get_last_ingestion_status(object_name)
        # self.pipeline.ingest_schema()
        # self.pipeline.ingest_raw_data()

    def execute(
        self,
        ingestion_config,
        credentials,
        concurrent=False,
    ):
        # Setup basic pipeline attributes
        self.pipeline = Pipeline(ingestion_config, credentials)
        self.pipeline.setup()

        # Conditionals if concurrent runner or not
        if concurrent:
            return
        return