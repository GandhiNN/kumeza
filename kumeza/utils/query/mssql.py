import pkgutil

from jinja2 import Template

from kumeza.config.data_assets.data_assets_config import AssetsId
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

    def __init__(
        self,
        source_system_config: SourceSystemConfig,
        assets_id: AssetsId,
    ):
        super().__init__(source_system_config=source_system_config)
        self.assets_id = assets_id

    def _render_custom_query(self):
        return self.assets_id.assets[0].custom_query

    def _render_standard_query(self):
        template = pkgutil.get_data(__package__, "models/mssql/standard.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{self.assets_id.database_name}.{self.assets_id.assets[0].database_schema}.{self.assets_id.assets[0].asset_name}"
        )

    def _render_incremental_query(self, **render_opt):
        template = pkgutil.get_data(__package__, "models/mssql/incremental.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{self.assets_id.database_name}.{self.assets_id.assets[0].database_schema}.{self.assets_id.assets[0].asset_name}",
            incremental_col=f"{self.assets_id.assets[0].incremental_column}",
            start_time=render_opt["start_time"],
            end_time=render_opt["end_time"],
        )

    def _render_schema_query(self):
        template = pkgutil.get_data(__package__, "models/mssql/schema.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{self.assets_id.database_name}.{self.assets_id.assets[0].database_schema}.{self.assets_id.assets[0].asset_name}"
        )

    def get_sql_query(self, mode: str = "standard", **render_opt) -> str:
        if mode == "schema":
            return self._render_schema_query()
        if mode == "standard":
            return self._render_standard_query()
        if mode == "custom":
            return self._render_custom_query()
        if mode == "incremental":
            return self._render_incremental_query(**render_opt)
        raise ValueError("Query logic not implemented yet!")
