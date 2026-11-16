from pathlib import Path

import pandas as pd
import pytest
import xlwt
from psycopg2 import sql
from sqlalchemy import create_engine, text

from geodatalab_ingestion.core.ingestion_runner import IngestionRunner
from geodatalab_ingestion.core.models import DestinationConfig, SourceConfig, SourceData
from geodatalab_ingestion.core.sources.http_source import HttpSource
from geodatalab_ingestion.core.sources.local_source import LocalSource
from geodatalab_ingestion.loaders.geographic.ogr_loader import OgrLoader
from geodatalab_ingestion.loaders.tabular import pandas_loader
from geodatalab_ingestion.loaders.tabular.pandas_loader import PandasLoader


@pytest.fixture
def database(tmp_path, monkeypatch):
    database_path = tmp_path / "test.sqlite"
    engine = create_engine(f"sqlite:///{database_path}")
    monkeypatch.setattr(pandas_loader, "create_engine", lambda url: engine)
    monkeypatch.setattr(
        PandasLoader, "_ensure_schema", staticmethod(lambda connection, schema: None)
    )
    monkeypatch.setattr(
        OgrLoader,
        "_get_database_environment",
        lambda self: {
            "PGHOST": "localhost",
            "PGPORT": "5432",
            "PGDATABASE": "test",
            "PGUSER": "test",
            "PGPASSWORD": "test",
        },
    )
    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: None)
    yield engine
    engine.dispose()


def rows(engine, table):
    with engine.connect() as connection:
        return connection.execute(text(f'SELECT * FROM "{table}" ORDER BY id')).all()


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_csv_ingestion_from_local_and_http(tmp_path, database, source_type):
    path = tmp_path / "donnees.csv"
    path.write_text("id;nom\n1;Lyon\n2;Paris\n", encoding="utf-8")
    options = {"read_options": {"sep": ";", "encoding": "utf-8"}}

    if source_type == "local":
        source = LocalSource().extract(SourceConfig("data", "local", str(path), options))
    else:
        class FakeDownload:
            def download(self, url, destination, headers):
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                return destination

        options["download_dir"] = str(tmp_path / "downloads")
        source = HttpSource(FakeDownload()).extract(
            SourceConfig("data", "http", "https://example.test/donnees.csv", options)
        )

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    assert result.source_feature_count == 2
    assert result.destination_record_count == 2
    assert result.records_inserted == 2
    assert rows(database, "donnees") == [(1, "Lyon"), (2, "Paris")]


@pytest.mark.parametrize("extension", ["xlsx", "xls"])
@pytest.mark.parametrize("source_type", ["local", "http"])
def test_excel_all_sheets_are_loaded(tmp_path, database, extension, source_type):
    path = tmp_path / f"population.{extension}"
    if extension == "xlsx":
        with pd.ExcelWriter(path) as writer:
            pd.DataFrame({"id": [1], "nom": ["Lyon"]}).to_excel(
                writer, sheet_name="Villes", index=False
            )
            pd.DataFrame({"id": [2], "nom": ["France"]}).to_excel(
                writer, sheet_name="Pays", index=False
            )
    else:
        workbook = xlwt.Workbook()
        for sheet_name, record in [("Villes", (1, "Lyon")), ("Pays", (2, "France"))]:
            sheet = workbook.add_sheet(sheet_name)
            for column, value in enumerate(("id", "nom")):
                sheet.write(0, column, value)
            for column, value in enumerate(record):
                sheet.write(1, column, value)
        workbook.save(str(path))

    if source_type == "local":
        source = LocalSource().extract(SourceConfig("population", "local", str(path)))
    else:
        class FakeDownload:
            def download(self, url, destination, headers):
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                return destination

        source = HttpSource(FakeDownload()).extract(
            SourceConfig(
                "population",
                "http",
                "https://example.test/download",
                {"download_dir": str(tmp_path / "downloads"), "filename": path.name},
            )
        )

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    assert list(result.layer_results) == ["Villes", "Pays"]
    assert result.records_inserted == 2
    assert rows(database, "population_villes") == [(1, "Lyon")]
    assert rows(database, "population_pays") == [(2, "France")]


def test_loading_modes_and_counts(tmp_path, database):
    path = tmp_path / "data.csv"
    path.write_text("id,nom\n1,Lyon\n", encoding="utf-8")
    source = SourceData("data", "local", path, format="csv")
    loader = PandasLoader()

    def load(mode):
        return loader.load(source, DestinationConfig("postgis", "main", "data", {"mode": mode}))

    assert load("create").records_inserted == 1
    with pytest.raises(ValueError, match="already exists"):
        load("create")
    appended = load("append")
    assert (appended.records_inserted, appended.destination_record_count) == (1, 2)
    path.write_text("id,nom\n2,Paris\n", encoding="utf-8")
    overwritten = load("overwrite")
    assert (overwritten.records_inserted, overwritten.destination_record_count) == (1, 1)
    assert rows(database, "data") == [(2, "Paris")]


def test_append_missing_table_and_invalid_options_fail(tmp_path, database):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    source = SourceData("data", "local", path)
    loader = PandasLoader()
    with pytest.raises(ValueError, match="does not exist"):
        loader.load(source, DestinationConfig("postgis", "main", "missing", {"mode": "append"}))
    source.metadata["read_options"] = {"chunksize": 1}
    with pytest.raises(ValueError, match="Unsupported read options"):
        loader.load(source, DestinationConfig("postgis", "main", "data"))


def test_runner_reports_csv_read_errors(tmp_path, database):
    path = tmp_path / "empty.csv"
    path.touch()
    source = SourceData("empty", "local", path)

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert not result.success
    assert result.failed_layers == 1
    assert result.layer_results["empty"].error


def test_failed_append_rolls_back(tmp_path, database):
    path = tmp_path / "data.csv"
    path.write_text("id,nom\n2,Paris\n1,Lyon\n", encoding="utf-8")
    with database.begin() as connection:
        connection.execute(text("CREATE TABLE data (id INTEGER PRIMARY KEY, nom TEXT)"))
        connection.execute(text("INSERT INTO data VALUES (1, 'Existing')"))

    with pytest.raises(pd.errors.DatabaseError, match="UNIQUE constraint"):
        PandasLoader().load(
            SourceData("data", "local", path),
            DestinationConfig("postgis", "main", "data", {"mode": "append"}),
        )

    assert rows(database, "data") == [(1, "Existing")]


def test_postgres_tabular_modes(postgis, tmp_path):
    connection, schema = postgis
    path = tmp_path / "data.csv"
    path.write_text("id,nom\n1,Lyon\n", encoding="utf-8")
    source = SourceData("data", "local", path)
    loader = PandasLoader()

    for mode, expected_count in [("create", 1), ("append", 2), ("overwrite", 1)]:
        result = loader.load(source, DestinationConfig("postgis", schema, "data", {"mode": mode}))
        assert result.success
        assert result.destination_record_count == expected_count
        assert result.records_inserted == 1

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("ALTER TABLE {}.data ADD PRIMARY KEY (id)").format(
                sql.Identifier(schema)
            )
        )
    path.write_text("id,nom\n2,Paris\n1,Lyon\n", encoding="utf-8")
    with pytest.raises(pd.errors.DatabaseError):
        loader.load(source, DestinationConfig("postgis", schema, "data", {"mode": "append"}))
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("SELECT id, nom FROM {}.data").format(sql.Identifier(schema)))
        assert cursor.fetchall() == [(1, "Lyon")]
