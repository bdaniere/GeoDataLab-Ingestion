import json
import sqlite3
import subprocess
import zipfile
from contextlib import closing
from pathlib import Path

import pytest
from geodatalab_ingestion.core.ingestion_runner import IngestionRunner
from geodatalab_ingestion.core.models import DestinationConfig, SourceData
from geodatalab_ingestion.loaders.geographic.ogr_loader import OgrLoader
from psycopg2 import sql


def destination(schema, mode="create"):
    return DestinationConfig("postgis", schema, "features", options={"mode": mode})


def imported_rows(connection, schema):
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT id, nom, ST_SRID(geom) FROM {}.features ORDER BY id, nom").format(
                sql.Identifier(schema)
            )
        )
        return cursor.fetchall()


def expected_rows(geopackage):
    with closing(sqlite3.connect(geopackage)) as connection:
        rows = connection.execute("SELECT id, nom FROM villes ORDER BY id, nom").fetchall()
    return [(feature_id, name, 4326) for feature_id, name in rows]


def imported_monuments(connection, schema):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT f_geometry_column FROM geometry_columns "
            "WHERE f_table_schema = %s AND f_table_name = %s",
            (schema, "features"),
        )
        geometry_columns = cursor.fetchall()
        assert len(geometry_columns) == 1
        geometry = sql.Identifier(geometry_columns[0][0])
        cursor.execute(
            sql.SQL(
                "SELECT nom, categorie, ST_SRID({geometry}), "
                "ST_X({geometry}), ST_Y({geometry}) "
                "FROM {schema}.features ORDER BY nom"
            ).format(geometry=geometry, schema=sql.Identifier(schema))
        )
        return cursor.fetchall()


def assert_monuments(rows, count):
    assert len(rows) == count
    for name, category, srid, x, y in rows:
        assert (name, category, srid) == ("Tour Eiffel", "Monument", 4326)
        assert (x, y) == pytest.approx((2.2945, 48.8584))


def test_zipped_shapefile_mixed_polygons_in_all_modes(tmp_path, postgis, monkeypatch):
    connection, schema = postgis
    ring1 = [[2, 48], [2, 49], [3, 49], [3, 48], [2, 48]]
    ring2 = [[4, 48], [4, 49], [5, 49], [5, 48], [4, 48]]
    source_json = tmp_path / "source.geojson"
    source_json.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": 1},
                        "geometry": {"type": "Polygon", "coordinates": [ring1]},
                    },
                    {
                        "type": "Feature",
                        "properties": {"id": 2},
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [[ring1], [ring2]],
                        },
                    },
                ],
            }
        )
    )
    shapefile = tmp_path / "communes.shp"
    subprocess.run(
        ["ogr2ogr", "-f", "ESRI Shapefile", str(shapefile), str(source_json)],
        check=True,
        capture_output=True,
        text=True,
    )
    archive_path = tmp_path / "communes.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for path in tmp_path.glob("communes.*"):
            if path != archive_path:
                archive.write(path, f"data/{path.name}")

    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: None)
    source = SourceData(
        "communes",
        "local",
        archive_path,
        "zip",
        {"archive_member": "data/communes.shp"},
    )
    without_promotion = IngestionRunner().run(
        source,
        DestinationConfig(
            "postgis", schema, options={"mode": "create", "promote_to_multi": False}
        ),
    )
    assert not without_promotion.success
    assert "MultiPolygon" in without_promotion.layer_results["communes"].error

    for mode, expected_count in [("create", 2), ("append", 4), ("overwrite", 2)]:
        result = IngestionRunner().run(
            source,
            DestinationConfig(
                "postgis", schema, options={"mode": mode, "target_crs": 2154}
            ),
        )
        assert result.success, result.layer_results
        assert result.records_inserted == 2
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT f_geometry_column, type, srid FROM geometry_columns "
                "WHERE f_table_schema = %s AND f_table_name = %s",
                (schema, "communes"),
            )
            geometry, geometry_type, srid = cursor.fetchone()
            assert (geometry_type, srid) == ("MULTIPOLYGON", 2154)
            cursor.execute(
                sql.SQL(
                    "SELECT COUNT(*), COUNT(*) FILTER "
                    "(WHERE ST_GeometryType({geometry}) = 'ST_MultiPolygon') "
                    "FROM {schema}.communes"
                ).format(geometry=sql.Identifier(geometry), schema=sql.Identifier(schema))
            )
            assert cursor.fetchone() == (expected_count, expected_count)
            cursor.execute(
                sql.SQL(
                    "SELECT id, ST_NumGeometries({geometry}) "
                    "FROM {schema}.communes ORDER BY id"
                ).format(geometry=sql.Identifier(geometry), schema=sql.Identifier(schema))
            )
            repeat = expected_count // 2
            assert cursor.fetchall() == [(1, 1)] * repeat + [(2, 2)] * repeat


def test_explicit_promotion_for_point_geojson(postgis):
    connection, schema = postgis
    source = SourceData(
        "test_geojson",
        "local",
        Path(__file__).parent / "testdata" / "test.geojson",
        "geojson",
        {"source_layer": "test"},
    )
    result = OgrLoader().load(
        source,
        DestinationConfig("postgis", schema, "features", {"promote_to_multi": True}),
    )
    assert result.success, result.error
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT f_geometry_column, type FROM geometry_columns "
            "WHERE f_table_schema = %s AND f_table_name = %s",
            (schema, "features"),
        )
        geometry, geometry_type = cursor.fetchone()
        assert geometry_type == "MULTIPOINT"
        cursor.execute(
            sql.SQL("SELECT ST_GeometryType({}) FROM {}.features").format(
                sql.Identifier(geometry), sql.Identifier(schema)
            )
        )
        assert cursor.fetchall() == [("ST_MultiPoint",)]


def test_import_vector_formats(postgis, vector_source):
    connection, schema = postgis

    result = OgrLoader().load(vector_source, destination(schema))

    assert result.success, result.error
    assert result.records_processed == 1
    assert_monuments(imported_monuments(connection, schema), 1)


def test_create_vector_formats_rejects_existing_table(postgis, vector_source):
    connection, schema = postgis
    loader = OgrLoader()
    first = loader.load(vector_source, destination(schema))
    assert first.success, first.error

    with pytest.raises(ValueError, match="Destination table already exists"):
        loader.load(vector_source, destination(schema))

    assert_monuments(imported_monuments(connection, schema), 1)


def test_append_vector_formats(postgis, vector_source):
    connection, schema = postgis
    loader = OgrLoader()
    first = loader.load(vector_source, destination(schema))
    assert first.success, first.error

    result = loader.load(vector_source, destination(schema, "append"))

    assert result.success, result.error
    assert result.records_processed == 1
    assert_monuments(imported_monuments(connection, schema), 2)


def test_overwrite_vector_formats(postgis, vector_source):
    connection, schema = postgis
    loader = OgrLoader()
    first = loader.load(vector_source, destination(schema))
    assert first.success, first.error
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("UPDATE {}.features SET nom = 'obsolete'").format(
                sql.Identifier(schema)
            )
        )

    result = loader.load(vector_source, destination(schema, "overwrite"))

    assert result.success, result.error
    assert result.records_processed == 1
    assert_monuments(imported_monuments(connection, schema), 1)


def test_import_geopackage(postgis, source, geopackage):
    connection, schema = postgis

    result = OgrLoader().load(source, destination(schema))

    assert result.success, result.error
    assert result.records_processed == 2
    assert imported_rows(connection, schema) == expected_rows(geopackage)
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT ST_X(geom), ST_Y(geom) FROM {}.features ORDER BY id").format(
                sql.Identifier(schema)
            )
        )
        coordinates = cursor.fetchall()
    assert coordinates[0] == pytest.approx((2.3522, 48.8566))
    assert coordinates[1] == pytest.approx((4.8357, 45.764))


def test_create_rejects_existing_table(postgis, source, geopackage):
    connection, schema = postgis
    loader = OgrLoader()
    first = loader.load(source, destination(schema))
    assert first.success, first.error

    with pytest.raises(ValueError, match="Destination table already exists"):
        loader.load(source, destination(schema))

    assert imported_rows(connection, schema) == expected_rows(geopackage)


def test_append_adds_features(postgis, source, geopackage):
    connection, schema = postgis
    loader = OgrLoader()
    first = loader.load(source, destination(schema))
    assert first.success, first.error

    result = loader.load(source, destination(schema, "append"))

    assert result.success, result.error
    assert imported_rows(connection, schema) == sorted(expected_rows(geopackage) * 2)


def test_overwrite_replaces_table(postgis, source, geopackage):
    connection, schema = postgis
    loader = OgrLoader()
    first = loader.load(source, destination(schema))
    assert first.success, first.error
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("ALTER TABLE {}.features ADD COLUMN obsolete text").format(
                sql.Identifier(schema)
            )
        )
        cursor.execute(
            sql.SQL("UPDATE {}.features SET nom = 'obsolete'").format(
                sql.Identifier(schema)
            )
        )

    result = loader.load(source, destination(schema, "overwrite"))

    assert result.success, result.error
    assert imported_rows(connection, schema) == expected_rows(geopackage)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'features' AND column_name = 'obsolete'
            """,
            (schema,),
        )
        assert cursor.fetchone()[0] == 0


def test_missing_source_layer_raises_error(source, ogrinfo):
    source.metadata["source_layer"] = "missing_layer"

    with pytest.raises(ValueError, match="Source layer 'missing_layer' does not exist"):
        OgrLoader().load(source, destination("unused"))


@pytest.mark.parametrize("target_crs", ["not-an-epsg", 999999, True])
def test_invalid_target_crs_raises_error(geopackage, target_crs):
    source = SourceData("test_geopackage", "local", geopackage, format="gpkg")
    config = destination("unused")
    config.options["target_crs"] = target_crs

    with pytest.raises(ValueError, match="Invalid target CRS"):
        OgrLoader().load(source, config)
