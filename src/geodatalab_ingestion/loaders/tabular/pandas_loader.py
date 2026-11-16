from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL, Connection

from geodatalab_ingestion.core.models import (
    DestinationConfig,
    IngestionResult,
    SourceData,
)
from geodatalab_ingestion.loaders.base import BaseLoader
from geodatalab_ingestion.loaders.geographic.ogr_loader import OgrLoader


class PandasLoader(BaseLoader):
    """Load CSV files and Excel worksheets into PostgreSQL."""

    @staticmethod
    def _ensure_schema(connection: Connection, schema: str) -> None:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    def load(
        self,
        source: SourceData,
        destination: DestinationConfig,
    ) -> IngestionResult:
        started_at = datetime.now(UTC)

        if destination.destination_type != "postgis":
            raise NotImplementedError(
                f"Unsupported destination type: {destination.destination_type}"
            )
        if destination.table is None:
            raise ValueError("A destination table is required.")
        OgrLoader._validate_destination_identifiers(
            destination.schema, destination.table
        )
        if not isinstance(source.content, (str, Path)):
            raise TypeError("PandasLoader expects a local file path.")

        source_path = Path(source.content)
        if not source_path.is_file():
            raise FileNotFoundError(f"Source file does not exist: {source_path}")

        source_format = (source.format or source_path.suffix.lstrip(".")).lower()
        mode = destination.options.get("mode", "create")
        if mode not in {"create", "overwrite", "append"}:
            raise ValueError(f"Unsupported loading mode: {mode}")

        read_options = source.metadata.get("read_options", {})
        if not isinstance(read_options, dict):
            raise ValueError("read_options must be a dictionary.")
        allowed_options = (
            {"sep", "encoding", "decimal", "dtype", "header"}
            if source_format == "csv"
            else {"dtype", "header"}
        )
        unsupported_options = set(read_options) - allowed_options
        if unsupported_options:
            raise ValueError(f"Unsupported read options: {sorted(unsupported_options)}")

        layer = source.metadata.get("source_layer")
        if source_format == "csv":
            frame = pd.read_csv(source_path, **read_options)
        elif source_format in {"xlsx", "xls"}:
            if not isinstance(layer, str) or not layer:
                raise ValueError("An Excel sheet name is required in source_layer.")
            frame = pd.read_excel(source_path, sheet_name=layer, **read_options)
        else:
            raise ValueError(f"Unsupported tabular format: {source_format}")

        environment = OgrLoader()._get_database_environment()
        url = URL.create(
            "postgresql+psycopg2",
            username=environment["PGUSER"],
            password=environment["PGPASSWORD"],
            host=environment["PGHOST"],
            port=int(environment["PGPORT"]),
            database=environment["PGDATABASE"],
        )
        engine = create_engine(url)
        try:
            with engine.begin() as connection:
                self._ensure_schema(connection, destination.schema)
                exists = inspect(connection).has_table(
                    destination.table, schema=destination.schema
                )
                if mode == "create" and exists:
                    raise ValueError(
                        f"Destination table already exists: "
                        f"{destination.schema}.{destination.table}. "
                        "Use 'append' or 'overwrite' instead."
                    )
                if mode == "append" and not exists:
                    raise ValueError(
                        f"Destination table does not exist: "
                        f"{destination.schema}.{destination.table}."
                    )

                count_query = text(
                    f'SELECT COUNT(*) FROM "{destination.schema}"."{destination.table}"'
                )
                records_before = (
                    connection.execute(count_query).scalar_one() if exists else None
                )
                if_exists: Literal["fail", "replace", "append"]
                if mode == "overwrite":
                    if_exists = "replace"
                elif mode == "append":
                    if_exists = "append"
                else:
                    if_exists = "fail"
                frame.to_sql(
                    destination.table,
                    con=connection,
                    schema=destination.schema,
                    if_exists=if_exists,
                    index=False,
                )
                records_after = connection.execute(count_query).scalar_one()
        finally:
            engine.dispose()

        return IngestionResult(
            source_name=source.source_name,
            success=True,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            records_processed=len(frame),
            source_feature_count=len(frame),
            destination_record_count=records_after,
            records_inserted=(
                records_after - records_before if mode == "append" else records_after
            ),
            metadata={
                "source_file": str(source_path.resolve()),
                "source_layer": layer,
                "destination_schema": destination.schema,
                "destination_table": destination.table,
                "loading_mode": mode,
                "records_before": records_before,
            },
        )
