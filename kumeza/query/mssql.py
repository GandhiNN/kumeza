import pkgutil

from jinja2 import Template

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
    ):
        super().__init__(source_system_config=source_system_config)

    def _render_custom_query(self, **render_opt):
        return render_opt["custom_query"]

    def _render_stored_procedures(self, **render_opt):
        template = pkgutil.get_data(__package__, "models/mssql/stored_procedures.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{render_opt['database_name']}.{render_opt['database_schema']}.{render_opt['object_name']}",
            start_time=render_opt["start_time"],
        )

    def _render_standard_query(self, **render_opt):
        template = pkgutil.get_data(__package__, "models/mssql/standard.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{render_opt['database_name']}.{render_opt['database_schema']}.{render_opt['object_name']}"
        )

    def _render_incremental_query(self, **render_opt):
        template = pkgutil.get_data(__package__, "models/mssql/incremental.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{render_opt['database_name']}.{render_opt['database_schema']}.{render_opt['object_name']}",
            incremental_col=f"{render_opt['incremental_column']}",
            start_time=render_opt["start_time"],
            end_time=render_opt["end_time"],
        )

    def _render_schema_query(self, **render_opt):
        template = pkgutil.get_data(__package__, "models/mssql/schema.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{render_opt['database_name']}.{render_opt['database_schema']}.{render_opt['object_name']}"
        )

    def _render_row_count_query(self, **render_opt):
        template = pkgutil.get_data(__package__, "models/mssql/row_count.sql")
        return Template(template.decode("utf-8")).render(
            source=f"{render_opt['database_name']}.{render_opt['database_schema']}.{render_opt['object_name']}"
        )

    def get_query(self, mode: str = "standard", **render_opt) -> str:
        if mode == "schema":
            return self._render_schema_query(**render_opt)
        if mode == "standard":
            return self._render_standard_query(**render_opt)
        if mode == "row_count":
            return self._render_row_count_query(**render_opt)
        if mode == "custom":
            return self._render_custom_query(**render_opt)
        if mode == "incremental":
            return self._render_incremental_query(**render_opt)
        if mode == "stored_procedures":
            return self._render_stored_procedures(**render_opt)
        raise ValueError("Query logic not implemented yet!")
