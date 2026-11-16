import logging
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from geodatalab_ingestion.core.ingestion_history import IngestionHistory
from geodatalab_ingestion.core.models import (
    DestinationConfig,
    IngestionResult,
    MultiLayerIngestionResult,
    SourceData,
)
from geodatalab_ingestion.core.table_naming import generate_table_names
from geodatalab_ingestion.extraction.archive_manager import ArchiveManager
from geodatalab_ingestion.extraction.layer_discovery import LayerDiscovery
from geodatalab_ingestion.loaders.geographic.ogr_loader import OgrLoader
from geodatalab_ingestion.loaders.tabular.pandas_loader import PandasLoader

logger = logging.getLogger(__name__)


class IngestionRunner:
    """Orchestrate ingestion of geographic layers and tabular files."""

    def __init__(
        self,
        loader: OgrLoader | None = None,
        layer_discovery: LayerDiscovery | None = None,
        pandas_loader: PandasLoader | None = None,
    ):
        self.loader = loader or OgrLoader()
        self.layer_discovery = layer_discovery or LayerDiscovery()
        self.pandas_loader = pandas_loader or PandasLoader()

    def _initialize_history(self) -> IngestionHistory | None:
        """Initialize persistent ingestion history.

        Returns:
            IngestionHistory | None: History manager if available.
        """

        try:
            environment = self.loader._get_database_environment()

            history = IngestionHistory(environment)
            history.initialize()

            return history

        except Exception:
            logger.exception(
                "Unable to initialize ingestion history. "
                "The ingestion will continue without history."
            )
            return None

    def run(
        self,
        source: SourceData,
        destination: DestinationConfig,
    ) -> MultiLayerIngestionResult:
        """Discover and load all layers from a source file.

        Args:
            source: Extracted source data.
            destination: Destination configuration.

        Returns:
            MultiLayerIngestionResult: Global and per-layer results.
        """
        if destination.destination_type != "postgis":
            raise NotImplementedError(
                f"Unsupported destination type: {destination.destination_type}"
            )

        if destination.table is not None:
            raise ValueError(
                "Destination table must not be specified for multi-layer ingestion."
            )

        started_at = datetime.now(UTC)
        with ArchiveManager().prepare(source) as prepared_source:
            return self._run_prepared(prepared_source, destination, started_at)

    def _run_prepared(
        self,
        source: SourceData,
        destination: DestinationConfig,
        started_at: datetime,
    ) -> MultiLayerIngestionResult:
        source_path = Path(source.content)
        source_format = (source.format or source_path.suffix.lstrip(".")).lower()
        is_tabular = source_format in {"csv", "xlsx", "xls"}

        if source_format == "csv":
            layer_names = [source_path.stem]
        elif source_format in {"xlsx", "xls"}:
            with pd.ExcelFile(source_path) as workbook:
                sheet_names = workbook.sheet_names
                if any(not isinstance(name, str) for name in sheet_names):
                    raise ValueError("Excel sheet names must be strings.")
                layer_names = [name for name in sheet_names if isinstance(name, str)]
        else:
            layer_names = [
                layer.name for layer in self.layer_discovery.discover(source.content)
            ]

        table_names = generate_table_names(
            source_path=source.content,
            layer_names=layer_names,
        )

        loading_mode = destination.options.get("mode", "create")

        logger.info(
            "Ingestion started | source=%s | layers=%d | schema=%s | mode=%s",
            source.source_name,
            len(layer_names),
            destination.schema,
            loading_mode,
        )

        # Initialize persistent history.
        history = self._initialize_history()
        ingestion_id = None

        if history is not None:
            try:
                ingestion_id = history.start_ingestion(
                    source_name=source.source_name,
                    source_path=str(source.metadata.get("archive_path", source.content)),
                    destination_schema=destination.schema,
                    loading_mode=loading_mode,
                    started_at=started_at,
                    metadata=source.metadata,
                )

                logger.info(
                    "Ingestion history created | ingestion_id=%s",
                    ingestion_id,
                )

            except Exception:
                logger.exception("Unable to create global ingestion history entry.")
                history = None

        layer_results = {}

        for layer_name in layer_names:
            table_name = table_names[layer_name]

            layer_source = replace(
                source,
                metadata={
                    **source.metadata,
                    "source_layer": layer_name,
                },
            )

            layer_destination = DestinationConfig(
                destination_type=destination.destination_type,
                schema=destination.schema,
                table=table_name,
                options=destination.options.copy(),
            )

            logger.info(
                "Layer ingestion started | source=%s | layer=%s | table=%s",
                source.source_name,
                layer_name,
                table_name,
            )

            try:
                result = (self.pandas_loader if is_tabular else self.loader).load(
                    source=layer_source,
                    destination=layer_destination,
                )

            except Exception as error:
                logger.exception(
                    "Unexpected error during layer ingestion | layer=%s",
                    layer_name,
                )

                result = IngestionResult(
                    source_name=source.source_name,
                    success=False,
                    started_at=datetime.now(UTC),
                    finished_at=datetime.now(UTC),
                    error=str(error),
                    metadata={
                        "source_layer": layer_name,
                        "destination_schema": destination.schema,
                        "destination_table": table_name,
                    },
                )

            layer_results[layer_name] = result

            if result.success:
                logger.info(
                    "Layer ingestion completed | layer=%s | duration=%s",
                    layer_name,
                    result.duration_seconds,
                )
            else:
                logger.warning(
                    "Layer ingestion failed | layer=%s | error=%s",
                    layer_name,
                    result.error,
                )

            # Persist individual layer result.
            if history is not None and ingestion_id is not None:
                try:
                    history.record_layer_result(
                        ingestion_id=ingestion_id,
                        layer_name=layer_name,
                        destination_table=table_name,
                        result=result,
                    )

                except Exception:
                    logger.exception(
                        "Unable to persist layer history | layer=%s",
                        layer_name,
                    )

        finished_at = datetime.now(UTC)

        success = all(result.success for result in layer_results.values())

        successful_layers = sum(result.success for result in layer_results.values())

        failed_layers = sum(not result.success for result in layer_results.values())

        # Calculate global counters.
        def total_count(attribute: str) -> int | None:
            values = [
                getattr(result, attribute, None) for result in layer_results.values()
            ]

            if not values or any(value is None for value in values):
                return None

            return sum(v for v in values if v is not None)

        source_count = total_count("source_feature_count")
        destination_count = total_count("destination_record_count")
        inserted_count = total_count("records_inserted")

        if successful_layers == len(layer_results):
            status = "success"
        elif successful_layers > 0:
            status = "partial"
        else:
            status = "failed"

        duration = (finished_at - started_at).total_seconds()

        logger.info(
            "Ingestion completed | source=%s | status=%s "
            "| successful_layers=%d | failed_layers=%d | duration=%.2fs",
            source.source_name,
            status,
            successful_layers,
            failed_layers,
            duration,
        )

        # Finalize global history.
        if history is not None and ingestion_id is not None:
            try:
                history.finish_ingestion(
                    ingestion_id=ingestion_id,
                    status=status,
                    finished_at=finished_at,
                    duration_seconds=duration,
                    source_feature_count=source_count,
                    destination_record_count=destination_count,
                    records_inserted=inserted_count,
                    metadata={
                        "successful_layers": successful_layers,
                        "failed_layers": failed_layers,
                    },
                )

            except Exception:
                logger.exception(
                    "Unable to finalize global ingestion history | id=%s",
                    ingestion_id,
                )

        return MultiLayerIngestionResult(
            source_name=source.source_name,
            success=success,
            started_at=started_at,
            finished_at=finished_at,
            layer_results=layer_results,
        )
