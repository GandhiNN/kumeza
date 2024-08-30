import pkgutil

from jinja2 import Template

from kumeza.config.data_assets.data_assets_config import Assets
from kumeza.config.source_system.source_system_config import SourceSystemConfig


class MSSQLQueryManager:

    def __init__(self, source_system_config: SourceSystemConfig):
        if source_system_config.type != "rdbms":
            raise TypeError("MS-SQL Query Manager only accepts RDBMS Database Type!")
        if "mssql" not in source_system_config.database_engine:
            raise ValueError(
                "MS-SQL Query Manager only accepts MS-SQL Database Engine!"
            )
        self.source_system_config = source_system_config
        self.time_format = "yyyy-mm-dd HH:MM:SS"


class MSSQLQueryTemplater(MSSQLQueryManager):

    def __init__(self, source_system_config: SourceSystemConfig, assets: Assets):
        super().__init__(source_system_config=source_system_config)
        self.assets = assets

    def _render_custom_query(self):
        return self.assets.custom_query

    def _render_standard_query(self):
        template = pkgutil.get_data(__package__, "models/mssql/standard.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{self.assets.database_name}.{self.assets.database_schema}.{self.assets.asset_name}"
        )

    def _render_schema_query(self):
        template = pkgutil.get_data(__package__, "models/mssql/header_only.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{self.assets.database_name}.{self.assets.database_schema}.{self.assets.asset_name}"
        )

    def get_sql_query(self, mode: str = "standard") -> str:
        if mode == "schema":
            return self._render_schema_query()
        if mode == "standard":
            return self._render_standard_query()
        if mode == "custom":
            return self._render_custom_query()
        raise ValueError("Query logic not implemented yet!")
