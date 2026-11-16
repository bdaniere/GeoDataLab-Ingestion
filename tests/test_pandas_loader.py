import pandas as pd
import pytest
from psycopg2 import sql
from sqlalchemy import text

from geodatalab_ingestion.core.ingestion_runner import IngestionRunner
from geodatalab_ingestion.core.models import DestinationConfig, SourceData
from geodatalab_ingestion.loaders.tabular.pandas_loader import PandasLoader


def rows(engine, table):
    with engine.connect() as connection:
        return connection.execute(text(f'SELECT * FROM "{table}" ORDER BY id')).all()


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_csv_ingestion_from_local_and_http(
    database, source_type, tabular_testdata, tabular_source
):
    path = tabular_testdata / "donnees.csv"
    options = {"read_options": {"sep": ";", "encoding": "utf-8"}}
    source = tabular_source(
        path, source_type, options, url="https://example.test/donnees.csv"
    )

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    assert result.source_feature_count == 2
    assert result.destination_record_count == 2
    assert result.records_inserted == 2
    assert rows(database, "donnees") == [(1, "Lyon"), (2, "Paris")]


@pytest.mark.parametrize("extension", ["xlsx", "xls"])
@pytest.mark.parametrize("source_type", ["local", "http"])
def test_excel_all_sheets_are_loaded(
    database, extension, source_type, tabular_testdata, tabular_source
):
    path = tabular_testdata / f"population.{extension}"
    source = tabular_source(path, source_type)

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    assert list(result.layer_results) == ["Villes", "Pays"]
    assert result.records_inserted == 2
    assert rows(database, "population_villes") == [(1, "Lyon")]
    assert rows(database, "population_pays") == [(2, "France")]


def test_loading_modes_and_counts(database, tabular_testdata):
    source = SourceData("data", "local", tabular_testdata / "lyon.csv", format="csv")
    loader = PandasLoader()

    def load(mode):
        return loader.load(
            source, DestinationConfig("postgis", "main", "data", {"mode": mode})
        )

    assert load("create").records_inserted == 1
    with pytest.raises(ValueError, match="already exists"):
        load("create")
    appended = load("append")
    assert (appended.records_inserted, appended.destination_record_count) == (1, 2)
    source.content = tabular_testdata / "paris.csv"
    overwritten = load("overwrite")
    assert (overwritten.records_inserted, overwritten.destination_record_count) == (
        1,
        1,
    )
    assert rows(database, "data") == [(2, "Paris")]


def test_append_missing_table_and_invalid_options_fail(database, tabular_testdata):
    source = SourceData("data", "local", tabular_testdata / "lyon.csv")
    loader = PandasLoader()
    with pytest.raises(ValueError, match="does not exist"):
        loader.load(
            source, DestinationConfig("postgis", "main", "missing", {"mode": "append"})
        )
    source.metadata["read_options"] = {"chunksize": 1}
    with pytest.raises(ValueError, match="Unsupported read options"):
        loader.load(source, DestinationConfig("postgis", "main", "data"))


def test_runner_reports_csv_read_errors(database, tabular_testdata):
    source = SourceData("empty", "local", tabular_testdata / "empty.csv")

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert not result.success
    assert result.failed_layers == 1
    assert result.layer_results["empty"].error


def test_failed_append_rolls_back(database, tabular_testdata):
    with database.begin() as connection:
        connection.execute(text("CREATE TABLE data (id INTEGER PRIMARY KEY, nom TEXT)"))
        connection.execute(text("INSERT INTO data VALUES (1, 'Existing')"))

    with pytest.raises(pd.errors.DatabaseError, match="UNIQUE constraint"):
        PandasLoader().load(
            SourceData("data", "local", tabular_testdata / "duplicate_id.csv"),
            DestinationConfig("postgis", "main", "data", {"mode": "append"}),
        )

    assert rows(database, "data") == [(1, "Existing")]


def test_postgres_tabular_modes(postgis, tabular_testdata):
    connection, schema = postgis
    source = SourceData("data", "local", tabular_testdata / "lyon.csv")
    loader = PandasLoader()

    for mode, expected_count in [("create", 1), ("append", 2), ("overwrite", 1)]:
        result = loader.load(
            source, DestinationConfig("postgis", schema, "data", {"mode": mode})
        )
        assert result.success
        assert result.destination_record_count == expected_count
        assert result.records_inserted == 1

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("ALTER TABLE {}.data ADD PRIMARY KEY (id)").format(
                sql.Identifier(schema)
            )
        )
    source.content = tabular_testdata / "duplicate_id.csv"
    with pytest.raises(pd.errors.DatabaseError):
        loader.load(
            source, DestinationConfig("postgis", schema, "data", {"mode": "append"})
        )
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT id, nom FROM {}.data").format(sql.Identifier(schema))
        )
        assert cursor.fetchall() == [(1, "Lyon")]
