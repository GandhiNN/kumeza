from kumeza.config.source_system.source_system_config import SourceSystemConfig


class InfluxQLQueryManager:
    def __init__(self, source_system_config: SourceSystemConfig):
        if source_system_config.type != "timeseries":
            raise TypeError(
                "InfluxQL Query Manager only accepts Time Series Database Type!"
            )
        if "influx" not in source_system_config.database_engine:
            raise ValueError(
                "InfluxQL Query Manager only accepts Influx Database Engine!"
            )
        self.source_system_config = source_system_config
        self.time_format = "yyyy-mm-dd HH:MM:SS"


class InfluxQLQueryTemplater(InfluxQLQueryManager):
    def __init__(
        self,
        source_system_config: SourceSystemConfig,
    ):
        super().__init__(source_system_config=source_system_config)

    def _render_custom_query(self, **render_opt):
        return render_opt["custom_query"]

    def get_query(self, mode: str = "custom", **render_opt) -> str:
        if mode == "custom":
            return self._render_custom_query(**render_opt)
        raise ValueError("Query logic not implemented yet!")
