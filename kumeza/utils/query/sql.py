from kumeza.config.data_assets.data_assets_config import Assets
from kumeza.config.source_system.source_system_config import SourceSystemConfig


class SQLQueryManager:

    def __init__(self, source_system_config: SourceSystemConfig, assets: Assets):
        if source_system_config.type == "rdbms":
            raise ValueError("SQL Query Manager only accepts RDBMS Database Type!")
        self.source_system_config = source_system_config
        self.assets = assets

    def get_sql_query(self) -> str:
        if self.source_system_config.database_engine == "mssql":
            if self.assets.custom_query is not None:
                return self.assets.custom_query
            if not self.assets.incremental:
                if self.assets.query_type == "standard":
                    return f"SELECT * FROM {self.assets.database_schema}.{self.assets.asset_name}"
        else:
            raise ValueError("Database engine not implemented yet!")
