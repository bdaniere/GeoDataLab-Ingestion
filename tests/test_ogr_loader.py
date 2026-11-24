import subprocess
from pathlib import Path

import pytest
from geodatalab_ingestion.core.models import DestinationConfig, SourceData
from geodatalab_ingestion.extraction.layer_discovery import LayerDiscovery, LayerInfo
from geodatalab_ingestion.loaders.geographic import ogr_loader
from geodatalab_ingestion.loaders.geographic.ogr_loader import OgrLoader


@pytest.mark.parametrize(
    "filename,geometry,setting,expected",
    [
        ("data.shp", "Polygon", None, True),
        ("data.SHP", "3D Polygon", None, True),
        ("data.shp", "Multi Polygon", None, True),
        ("data.shp", "Point", None, False),
        ("data.shp", "Line String", None, False),
        ("data.geojson", "Polygon", None, False),
        ("data.geojson", "Polygon", True, True),
        ("data.shp", "Polygon", False, False),
    ],
)
def test_promotion_policy(monkeypatch, filename, geometry, setting, expected):
    monkeypatch.setattr(
        LayerDiscovery, "discover", lambda self, path: [LayerInfo("data", geometry)]
    )
    assert OgrLoader._promote_to_multi(Path(filename), "data", setting) is expected


def test_shapefile_promotion_rejects_unknown_geometry(monkeypatch):
    monkeypatch.setattr(
        LayerDiscovery, "discover", lambda self, path: [LayerInfo("data", "Unknown")]
    )
    with pytest.raises(ValueError, match="Unable to determine shapefile geometry"):
        OgrLoader._promote_to_multi(Path("data.shp"), "data", None)


def test_validate_source_layer_handles_spaces(monkeypatch, tmp_path):
    monkeypatch.setattr(
        ogr_loader.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 0, stdout="1: layer with spaces\n2: another (Point)\n", stderr=""
        ),
    )
    OgrLoader._validate_source_layer(tmp_path / "source.geojson", "layer with spaces")


@pytest.mark.parametrize("value", ["true", 1, None, [], {}])
def test_promotion_option_must_be_boolean(tmp_path, value):
    source_path = tmp_path / "source.geojson"
    source_path.touch()
    with pytest.raises(ValueError, match="promote_to_multi must be a boolean"):
        OgrLoader().load(
            SourceData("source", "local", source_path),
            DestinationConfig(
                "postgis", "raw_data", "features", {"promote_to_multi": value}
            ),
        )


@pytest.fixture
def docker_env(tmp_path, monkeypatch):
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name, raising=False)
    env_path = tmp_path / "docker" / ".env"
    env_path.parent.mkdir()
    monkeypatch.setenv("GEODATALAB_ENV_FILE", str(env_path))
    return env_path


def test_database_environment_uses_application_environment(docker_env, monkeypatch):
    monkeypatch.delenv("GEODATALAB_ENV_FILE")
    monkeypatch.setenv("POSTGRES_DB", "environment_db")
    monkeypatch.setenv("POSTGRES_USER", "environment_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "environment_password")

    environment = OgrLoader()._get_database_environment()

    assert environment["PGHOST"] == "localhost"
    assert environment["PGPORT"] == "5432"
    assert environment["PGDATABASE"] == "environment_db"
    assert environment["PGUSER"] == "environment_user"
    assert environment["PGPASSWORD"] == "environment_password"


def test_database_environment_uses_docker_variables(docker_env, monkeypatch):
    docker_env.write_text(
        "POSTGRES_USER=file_user\n"
        "POSTGRES_PASSWORD=file_password\n"
        "POSTGRES_DB=file_db\n"
        "POSTGRES_PORT=5423\n"
    )
    monkeypatch.setenv("POSTGRES_USER", "environment_user")
    monkeypatch.setenv("UNRELATED_VARIABLE", "preserved")

    environment = OgrLoader()._get_database_environment()

    assert environment["PGHOST"] == "localhost"
    assert environment["PGPORT"] == "5423"
    assert environment["PGDATABASE"] == "file_db"
    assert environment["PGUSER"] == "environment_user"
    assert environment["PGPASSWORD"] == "file_password"
    assert environment["UNRELATED_VARIABLE"] == "preserved"


def test_database_environment_uses_configured_host_and_default_port(
    docker_env, monkeypatch
):
    docker_env.write_text(
        "POSTGRES_USER=user\nPOSTGRES_PASSWORD=password\nPOSTGRES_DB=db\n"
    )
    monkeypatch.setenv("POSTGRES_HOST", "database")

    environment = OgrLoader()._get_database_environment()

    assert environment["PGHOST"] == "database"
    assert environment["PGPORT"] == "5432"


def test_database_environment_uses_host_from_docker_file(docker_env):
    docker_env.write_text(
        "POSTGRES_HOST=database\n"
        "POSTGRES_USER=user\nPOSTGRES_PASSWORD=password\nPOSTGRES_DB=db\n"
    )

    environment = OgrLoader()._get_database_environment()

    assert environment["PGHOST"] == "database"


def test_database_environment_reports_missing_file(docker_env):
    with pytest.raises(FileNotFoundError, match="Environment file not found"):
        OgrLoader()._get_database_environment()


def test_database_environment_reports_missing_or_empty_variables(docker_env):
    docker_env.write_text("POSTGRES_USER=user\nPOSTGRES_PASSWORD=\n")

    with pytest.raises(ValueError) as error:
        OgrLoader()._get_database_environment()

    assert "POSTGRES_DB" in str(error.value)
    assert "POSTGRES_PASSWORD" in str(error.value)


@pytest.mark.parametrize("variable", ["POSTGRES_HOST", "POSTGRES_PORT"])
def test_database_environment_reports_empty_host_or_port(docker_env, variable):
    docker_env.write_text(
        f"POSTGRES_USER=user\nPOSTGRES_PASSWORD=password\nPOSTGRES_DB=db\n{variable}=\n"
    )

    with pytest.raises(ValueError, match=variable):
        OgrLoader()._get_database_environment()


def test_load_passes_database_environment_to_ogr2ogr(docker_env, tmp_path, monkeypatch):
    docker_env.write_text(
        "POSTGRES_USER=user\nPOSTGRES_PASSWORD=password\n"
        "POSTGRES_DB=db\nPOSTGRES_PORT=5423\n"
    )
    source_path = tmp_path / "source.gpkg"
    source_path.touch()
    source = SourceData("source", "local", source_path)
    destination = DestinationConfig("postgis", "raw_data", "features")
    captured = {}

    def fake_run(command, **kwargs):
        captured.update(kwargs["env"])
        return subprocess.CompletedProcess(command, 0, stdout="loaded", stderr="")

    monkeypatch.setattr(
        OgrLoader,
        "_get_source_feature_count",
        staticmethod(lambda source_path, source_layer: None),
    )
    monkeypatch.setattr(
        OgrLoader,
        "_ensure_destination_schema",
        staticmethod(lambda schema, environment: None),
    )
    monkeypatch.setattr(
        OgrLoader,
        "_destination_table_exists",
        staticmethod(lambda schema, table, environment: False),
    )
    monkeypatch.setattr(ogr_loader.subprocess, "run", fake_run)

    result = OgrLoader().load(source, destination)

    assert result.success
    assert captured["PGHOST"] == "localhost"
    assert captured["PGPORT"] == "5423"
    assert captured["PGDATABASE"] == "db"
    assert captured["PGUSER"] == "user"
    assert captured["PGPASSWORD"] == "password"


def test_load_creates_destination_schema(docker_env, tmp_path, monkeypatch):
    docker_env.write_text(
        "POSTGRES_USER=user\n"
        "POSTGRES_PASSWORD=password\n"
        "POSTGRES_DB=db\n"
        "POSTGRES_PORT=5423\n"
    )
    source_path = tmp_path / "source.gpkg"
    source_path.touch()
    source = SourceData("source", "local", source_path)
    destination = DestinationConfig(
        "postgis",
        "new_schema",
        "features",
    )
    called = {}

    def fake_ensure_destination_schema(schema, environment):
        called["schema"] = schema
        called["environment"] = environment

    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="loaded", stderr="")

    monkeypatch.setattr(
        OgrLoader,
        "_ensure_destination_schema",
        staticmethod(fake_ensure_destination_schema),
    )
    monkeypatch.setattr(
        OgrLoader,
        "_get_source_feature_count",
        staticmethod(lambda source_path, source_layer: None),
    )
    monkeypatch.setattr(
        OgrLoader,
        "_destination_table_exists",
        staticmethod(lambda schema, table, environment: False),
    )
    monkeypatch.setattr(ogr_loader.subprocess, "run", fake_run)

    result = OgrLoader().load(source, destination)

    assert result.success
    assert called["schema"] == "new_schema"
    assert called["environment"]["PGHOST"] == "localhost"
    assert called["environment"]["PGPORT"] == "5423"
    assert called["environment"]["PGDATABASE"] == "db"
    assert called["environment"]["PGUSER"] == "user"
