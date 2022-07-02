from enum import Enum
from typing import Dict, Optional

from pydantic import validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetPropertiesTransformer,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)


class Semantics(Enum):
    OVERWRITE = "OVERWRITE"
    PATCH = "PATCH"


class PatchDatasetPropertiesConfig(ConfigModel):
    properties: Optional[Dict[str, str]] = {}
    semantics: Semantics = Semantics.PATCH

    @validator("semantics", pre=True)
    def ensure_semantics_is_upper_case(cls, v):
        if isinstance(v, str):
            return v.upper()
        return v


class PatchDatasetProperties(DatasetPropertiesTransformer):
    ctx: PipelineContext
    config: PatchDatasetPropertiesConfig

    def __init__(self, config: PatchDatasetPropertiesConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        if self.config.semantics == Semantics.PATCH and self.ctx.graph is None:
            raise ConfigurationError(
                "With PATCH semantics, PatchDatasetProperties requires a datahub_api to connect to. Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe"
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "PatchDatasetProperties":
        config = PatchDatasetPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def get_properties_to_patch(
        graph: DataHubGraph, urn: str, properties_to_add: Dict[str, str]
    ) -> Optional[DatasetPropertiesClass]:
        server_properties = graph.get_aspect_v2(
            entity_urn=urn, aspect_type=DatasetPropertiesClass, aspect="datasetProperties"
        )
        dataset_properties = DatasetPropertiesClass(customProperties={})
        if server_properties:
            # patch new properties to existing properties
            dataset_properties.customProperties.update({
                **server_properties.customProperties,
                **properties_to_add
            })
        return dataset_properties

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        assert isinstance(mce.proposedSnapshot, DatasetSnapshotClass)
        dataset_properties = builder.get_aspect_if_available(mce, AspectType=DatasetPropertiesClass)
        if dataset_properties:
            to_update_custom_properties = {
                **dataset_properties.customProperties,
                **self.config.properties
            } if self.config and self.config.properties else dataset_properties.customProperties
            if to_update_custom_properties and self.config.semantics == Semantics.PATCH:
                assert self.ctx.graph
                patch_properties = PatchDatasetProperties.get_properties_to_patch(
                    self.ctx.graph, mce.proposedSnapshot.urn, to_update_custom_properties
                )
                builder.set_aspect(
                    mce, aspect=patch_properties, aspect_type=DatasetPropertiesClass
                )
        return mce
