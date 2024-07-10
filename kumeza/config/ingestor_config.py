import typing as t
from dataclasses import dataclass

from kumeza.config.credentials.credentials_config import CredentialsConfig
from kumeza.config.data_assets.data_assets_config import DataAssetsConfig
from kumeza.config.integration.integration_config import IntegrationConfig
from kumeza.config.metadata.metadata_config import MetadataConfig
from kumeza.config.runtime_environment.runtime_environment_config import (
    RuntimeEnvironmentConfig,
)
from kumeza.config.sinks.sinks_config import SinksConfig
from kumeza.config.source_system.source_system_config import SourceSystemConfig


@dataclass
class IngestionConfig:
    runtime_environment: RuntimeEnvironmentConfig
    source_system: SourceSystemConfig
    integration: IntegrationConfig
    credentials: CredentialsConfig
    metadata: MetadataConfig
    sinks: SinksConfig
    data_assets: DataAssetsConfig

    @classmethod
    def marshal(cls: t.Type["IngestionConfig"], obj: dict):
        return cls(
            runtime_environment=RuntimeEnvironmentConfig.marshal(
                obj["runtime_environment"]
            ),
            source_system=SourceSystemConfig.marshal(obj["source_system"]),
            integration=IntegrationConfig.marshal(obj["integration"]),
            credentials=CredentialsConfig.marshal(obj["credentials"]),
            metadata=MetadataConfig.marshal(obj["metadata"]),
            sinks=SinksConfig.marshal(obj["sinks"]),
            data_assets=DataAssetsConfig.marshal(obj["data_assets"]),
        )
