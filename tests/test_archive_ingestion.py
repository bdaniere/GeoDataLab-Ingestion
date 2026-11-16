import stat
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from psycopg2 import sql
from sqlalchemy import text

from geodatalab_ingestion.core.ingestion_runner import IngestionRunner
from geodatalab_ingestion.core.models import (
    DestinationConfig,
    IngestionResult,
    SourceData,
)
from geodatalab_ingestion.extraction.archive_manager import ArchiveManager
from geodatalab_ingestion.extraction.layer_discovery import LayerDiscovery, LayerInfo


def make_zip(path, files):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return path


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_zip_csv_ingestion(database, tmp_path, tabular_source, source_type):
    archive = make_zip(
        tmp_path / "data.zip",
        {"nested/population.csv": "id;nom\n1;Lyon\n2;Paris\n", "notes.txt": "notes"},
    )
    source = tabular_source(archive, source_type, {"read_options": {"sep": ";"}})

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    assert result.records_inserted == 2
    with database.connect() as connection:
        assert connection.execute(
            text("SELECT * FROM population ORDER BY id")
        ).all() == [(1, "Lyon"), (2, "Paris")]
    assert source.content.exists()
    assert not Path(result.layer_results["population"].metadata["source_file"]).exists()


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_archive_member_option_selects_exact_file(
    database, tmp_path, tabular_source, source_type
):
    archive = make_zip(
        tmp_path / "data.zip",
        {"one/data.csv": "id\n1\n", "two/data.csv": "id\n2\n"},
    )
    source = tabular_source(archive, source_type, {"archive_member": "two/data.csv"})

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    with database.connect() as connection:
        assert connection.execute(text("SELECT * FROM data")).all() == [(2,)]


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_zip_excel_discovers_sheets(
    database, tmp_path, tabular_source, tabular_testdata, source_type
):
    archive = make_zip(
        tmp_path / "excel.zip",
        {"nested/population.xlsx": (tabular_testdata / "population.xlsx").read_bytes()},
    )
    source = tabular_source(archive, source_type)

    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))

    assert result.success
    assert list(result.layer_results) == ["Villes", "Pays"]
    assert result.records_inserted == 2


def test_zip_shapefile_keeps_sidecars_for_discovery_and_loading(
    tmp_path, monkeypatch, ogrinfo
):
    shapefile_dir = Path(__file__).parent / "testdata" / "test_shapefile"
    archive = make_zip(
        tmp_path / "shapes.zip",
        {
            f"nested/{p.name}": p.read_bytes()
            for p in shapefile_dir.iterdir()
            if p.is_file()
        },
    )
    seen = []

    class Loader:
        def load(self, source, destination):
            path = Path(source.content)
            assert path.with_suffix(".dbf").is_file()
            assert path.with_suffix(".shx").is_file()
            assert source.metadata["archive_member"] == "nested/test.shp"
            seen.append(path)
            now = datetime.now(UTC)
            return IngestionResult(source.source_name, True, now, now)

    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: None)
    runner = IngestionRunner(loader=Loader(), layer_discovery=LayerDiscovery())
    source = SourceData("shapes", "local", archive, format="zip")
    result = runner.run(source, DestinationConfig("postgis"))

    assert result.success
    assert list(result.layer_results) == ["test"]
    assert seen and not seen[0].exists()


def test_geo_discovery_and_loading_use_extracted_file_and_cleanup_on_failure(
    tmp_path, monkeypatch
):
    archive = make_zip(
        tmp_path / "geo.zip", {"dir/test.shp": b"shp", "dir/test.dbf": b"dbf"}
    )
    seen = []

    class Discovery:
        def discover(self, path):
            path = Path(path)
            assert path.read_bytes() == b"shp"
            assert path.with_suffix(".dbf").read_bytes() == b"dbf"
            seen.append(path)
            return [LayerInfo("test", "Point")]

    class Loader:
        def load(self, source, destination):
            assert source.content == seen[0]
            assert destination.table == "test"
            raise RuntimeError("load failed")

    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: None)
    result = IngestionRunner(loader=Loader(), layer_discovery=Discovery()).run(
        SourceData("geo", "local", archive, "zip"), DestinationConfig("postgis")
    )
    assert not result.success
    assert result.layer_results["test"].error == "load failed"
    assert not seen[0].exists()


def test_zip_shapefile_full_postgis_ingestion(tmp_path, postgis, monkeypatch):
    connection, schema = postgis
    shapefile_dir = Path(__file__).parent / "testdata" / "test_shapefile"
    archive = make_zip(
        tmp_path / "shapes.zip",
        {
            f"nested/{p.name}": p.read_bytes()
            for p in shapefile_dir.iterdir()
            if p.is_file()
        },
    )
    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: None)

    result = IngestionRunner().run(
        SourceData("shapes", "local", archive, "zip"),
        DestinationConfig("postgis", schema),
    )

    assert result.success, result.layer_results
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT f_geometry_column FROM geometry_columns "
            "WHERE f_table_schema = %s AND f_table_name = %s",
            (schema, "test"),
        )
        geometry_columns = cursor.fetchall()
        assert len(geometry_columns) == 1
        cursor.execute(
            sql.SQL(
                "SELECT nom, categorie, ST_SRID({geometry}) FROM {schema}.test"
            ).format(
                geometry=sql.Identifier(geometry_columns[0][0]),
                schema=sql.Identifier(schema),
            )
        )
        assert cursor.fetchall() == [("Tour Eiffel", "Monument", 4326)]


def test_cleanup_after_discovery_error(tmp_path):
    archive = make_zip(tmp_path / "data.zip", {"file.geojson": "{}"})
    seen = []

    class Discovery:
        def discover(self, path):
            seen.append(Path(path))
            assert seen[0].exists()
            raise RuntimeError("discovery failed")

    with pytest.raises(RuntimeError, match="discovery failed"):
        IngestionRunner(layer_discovery=Discovery()).run(
            SourceData("data", "local", archive, "zip"), DestinationConfig("postgis")
        )
    assert not seen[0].exists()


def test_archive_history_keeps_original_path_and_selected_member(tmp_path, monkeypatch):
    archive = make_zip(tmp_path / "data.zip", {"dir/data.csv": "id\n1\n"})
    recorded = {}

    class History:
        def start_ingestion(self, **kwargs):
            recorded.update(kwargs)
            return 1

        def record_layer_result(self, **kwargs):
            pass

        def finish_ingestion(self, **kwargs):
            pass

    class Loader:
        def load(self, source, destination):
            now = datetime.now(UTC)
            return IngestionResult(source.source_name, True, now, now)

    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: History())
    result = IngestionRunner(pandas_loader=Loader()).run(
        SourceData("data", "local", archive, "zip"), DestinationConfig("postgis")
    )
    assert result.success
    assert recorded["source_path"] == str(archive)
    assert recorded["metadata"]["archive_member"] == "dir/data.csv"


@pytest.mark.parametrize(
    "files,member,message",
    [
        ({"one.csv": "x", "two.csv": "x"}, None, "exactly one"),
        ({"notes.txt": "x"}, None, "exactly one"),
        ({"one.csv": "x"}, "missing.csv", "not found"),
        ({"one.csv": "x"}, "", "non-empty"),
        ({"one.csv": "x"}, 1, "non-empty"),
    ],
)
def test_selection_errors(tmp_path, files, member, message):
    archive = make_zip(tmp_path / "data.zip", files)
    source = SourceData("data", "local", archive, "zip", {"archive_member": member})
    with pytest.raises(ValueError, match=message), ArchiveManager().prepare(source):
        pass


@pytest.mark.parametrize(
    "name",
    [
        "../escape.csv",
        "/absolute.csv",
        "dir/../../escape.csv",
        "dir\\escape.csv",
        "C:/escape.csv",
        "dir/./file.csv",
        "dir//file.csv",
    ],
)
def test_unsafe_paths_are_rejected(tmp_path, name):
    archive = make_zip(tmp_path / "data.zip", {name: "data", "safe.csv": "data"})
    source = SourceData("data", "local", archive, "zip", {"archive_member": "safe.csv"})
    with (
        pytest.raises(ValueError, match="Unsafe ZIP member path"),
        ArchiveManager().prepare(source),
    ):
        pass
    assert not (tmp_path / "escape.csv").exists()


def test_symlinks_and_duplicate_members_are_rejected(tmp_path):
    archive = tmp_path / "data.zip"
    link = zipfile.ZipInfo("link")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(archive, "w") as file:
        file.writestr("safe.csv", "id\n1\n")
        file.writestr(link, "../outside")
    with (
        pytest.raises(ValueError, match="Unsupported ZIP member type"),
        ArchiveManager().prepare(SourceData("data", "local", archive, "zip")),
    ):
        pass

    with zipfile.ZipFile(archive, "w") as file:
        file.writestr("safe.csv", "first")
        with pytest.warns(UserWarning, match="Duplicate name"):
            file.writestr("safe.csv", "second")
    with (
        pytest.raises(ValueError, match="Duplicate ZIP member"),
        ArchiveManager().prepare(SourceData("data", "local", archive, "zip")),
    ):
        pass


def test_content_detection_and_non_archive_validation(tmp_path):
    archive = make_zip(tmp_path / "download", {"data.csv": "id\n1\n"})
    source = SourceData("data", "http", archive)
    with ArchiveManager().prepare(source) as prepared:
        assert prepared.format == "csv"
        assert prepared.content.is_file()
    assert not prepared.content.exists()

    explicit = make_zip(tmp_path / "other.zip", {"data.custom": "data"})
    with ArchiveManager().prepare(
        SourceData("data", "local", explicit, "zip", {"archive_member": "data.custom"})
    ) as prepared:
        assert prepared.format == "custom"
        assert prepared.content.read_text() == "data"

    plain = tmp_path / "data.csv"
    plain.write_text("id\n1\n")
    with (
        pytest.raises(ValueError, match="requires a ZIP or 7z"),
        ArchiveManager().prepare(
            SourceData("data", "local", plain, "csv", {"archive_member": "data.csv"})
        ),
    ):
        pass


def test_corrupt_zip_and_size_limit_are_rejected(tmp_path, monkeypatch):
    archive = tmp_path / "data.zip"
    archive.write_bytes(b"not a zip")
    with (
        pytest.raises(zipfile.BadZipFile),
        ArchiveManager().prepare(SourceData("data", "local", archive, "zip")),
    ):
        pass
    make_zip(archive, {"data.csv": "id\n1\n"})
    monkeypatch.setattr(
        "geodatalab_ingestion.extraction.archive_manager.MAX_UNCOMPRESSED_SIZE", 3
    )
    with (
        pytest.raises(ValueError, match="maximum uncompressed size"),
        ArchiveManager().prepare(SourceData("data", "local", archive, "zip")),
    ):
        pass
    monkeypatch.setattr(
        "geodatalab_ingestion.extraction.archive_manager.MAX_UNCOMPRESSED_SIZE", 100
    )
    monkeypatch.setattr(
        "geodatalab_ingestion.extraction.archive_manager.MAX_ARCHIVE_FILES", 0
    )
    with (
        pytest.raises(ValueError, match="more than 0 entries"),
        ArchiveManager().prepare(SourceData("data", "local", archive, "zip")),
    ):
        pass
