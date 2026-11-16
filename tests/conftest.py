import os
import shutil
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from sqlalchemy import create_engine

from geodatalab_ingestion.core.download_manager import DownloadManager
from geodatalab_ingestion.core.ingestion_runner import IngestionRunner
from geodatalab_ingestion.core.models import SourceConfig, SourceData
from geodatalab_ingestion.core.sources.http_source import HttpSource
from geodatalab_ingestion.core.sources.local_source import LocalSource
from geodatalab_ingestion.loaders.geographic.ogr_loader import OgrLoader
from geodatalab_ingestion.loaders.tabular import pandas_loader
from geodatalab_ingestion.loaders.tabular.pandas_loader import PandasLoader


@pytest.fixture
def tabular_testdata():
    path = Path(__file__).parent / "testdata" / "tabular"
    assert path.is_dir(), f"Missing tabular test data: {path}"
    return path


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


@pytest.fixture
def tabular_source(tmp_path):
    def build_source(path, source_type, options=None, url=None):
        options = dict(options or {})
        if source_type == "local":
            return LocalSource().extract(SourceConfig("data", "local", str(path), options))
        if source_type != "http":
            raise ValueError(f"Unsupported test source type: {source_type}")

        class FakeDownload(DownloadManager):
            def download(
                self, url: str, destination: Path, headers: dict[str, str] | None = None
            ) -> Path:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
                return destination

        options["download_dir"] = str(tmp_path / "downloads")
        if url is None:
            url = "https://example.test/download"
            options["filename"] = path.name
        return HttpSource(FakeDownload()).extract(
            SourceConfig("data", "http", url, options)
        )

    return build_source


@pytest.fixture
def geopackage():
    path = Path(__file__).parent / "testdata" / "test.gpkg"
    assert path.is_file(), f"Missing test GeoPackage: {path}"
    return path


@pytest.fixture
def source(geopackage):
    return SourceData(
        "test_geopackage",
        "local",
        geopackage,
        format="gpkg",
        metadata={"source_layer": "villes"},
    )


@pytest.fixture(
    params=[
        ("test.geojson", "geojson"),
        ("test_shapefile/test.shp", "shp"),
    ],
    ids=["geojson", "shapefile"],
)
def vector_source(request):
    relative_path, file_format = request.param
    path = Path(__file__).parent / "testdata" / relative_path
    assert path.is_file(), f"Missing test file: {path}"
    return SourceData(
        f"test_{file_format}",
        "local",
        path,
        format=file_format,
        metadata={"source_layer": "test"},
    )


@pytest.fixture
def ogrinfo():
    if not shutil.which("ogrinfo"):
        if os.environ.get("TEST_POSTGIS_DSN"):
            pytest.fail("ogrinfo is required for PostGIS integration tests")
        pytest.skip("ogrinfo is required to inspect the GeoPackage")


@pytest.fixture
def postgis(monkeypatch, ogrinfo):
    dsn = os.environ.get("TEST_POSTGIS_DSN")
    if not dsn:
        pytest.skip("Set TEST_POSTGIS_DSN to run PostGIS integration tests")
    if not shutil.which("ogr2ogr"):
        pytest.fail("ogr2ogr is required for PostGIS integration tests")

    settings = parse_dsn(dsn)
    required = ("host", "port", "dbname", "user", "password")
    missing = [key for key in required if not settings.get(key)]
    if missing:
        pytest.fail(f"TEST_POSTGIS_DSN is missing: {', '.join(missing)}")

    environment = os.environ.copy()
    environment.update(
        {
            "PGHOST": settings["host"],
            "PGPORT": settings["port"],
            "PGDATABASE": settings["dbname"],
            "PGUSER": settings["user"],
            "PGPASSWORD": settings["password"],
        }
    )
    monkeypatch.setattr(OgrLoader, "_get_database_environment", lambda self: environment)

    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    schema = f"test_ogr_{uuid4().hex}"
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield connection, schema
    finally:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
        connection.close()
