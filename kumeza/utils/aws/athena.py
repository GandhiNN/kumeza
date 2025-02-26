import logging

from kumeza.utils.aws import BaseAwsUtil, boto_error_handler


logger = logging.getLogger(__name__)


class Athena(BaseAwsUtil):
    def __init__(self, region_name: str = "eu-west-1"):
        super().__init__(service_name="athena", region_name=region_name)

    @boto_error_handler(logger)
    def create_database(self, database_name: str, query_output_location: str) -> dict:
        """Create a database container in Athena

        Args:
            database_name (str): database name to be created
            query_output_location (str): s3 uri containing the execution output

        Returns:
            dict: Query execution result
        """
        logger.info(
            "Creating database %s. Query output location is: %s",
            database_name,
            query_output_location,
        )
        return self._create_boto_client().start_query_execution(
            QueryString=f"create database {database_name}",
            ResultConfiguration={"OutputLocation": query_output_location},
        )
