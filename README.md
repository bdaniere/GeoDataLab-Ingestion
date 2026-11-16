# GeoDataLab Ingestion

Python library for geospatial and tabular data ingestion into PostgreSQL/PostGIS.

## Features

- Local and HTTP data sources
- Geospatial layer discovery
- OGR-based ingestion into PostGIS
- CSV and Excel (.xlsx and .xls) ingestion using pandas
- ZIP and 7z archives from local and HTTP sources
- Multi-layer ingestion orchestration
- Ingestion history and reporting

## Requirements

- Python 3.13
- GDAL / OGR
- PostgreSQL with PostGIS

## Installation

The library is designed to be installed as a Python dependency using UV.

## Tabular ingestion

The existing `run_ingestion.py` entry point accepts local and HTTP CSV and
`.xlsx`/`.xls` files. The loader is selected from the file extension (for HTTP URLs
without an extension, set `source.options.filename`). Other formats continue to
use OGR. CSV files, including those with coordinate columns, are treated as
non-geographic tables; no geometry is created. CSV files are loaded into one
table; every Excel sheet is loaded into
its own table. Table names follow the same rules as geographic layers: a single
table uses the normalized `source.name`, while multiple sheets use the normalized
source name and sheet name. Name collisions are reported as errors.

Example configuration for a local CSV (replace `location` with an HTTP URL and
`type` with `http` for a remote file):

```json
{
  "source": {
    "name": "population",
    "type": "local",
    "location": "/path/to/population.csv",
    "options": {
      "read_options": {
        "sep": ";",
        "encoding": "utf-8"
      }
    }
  },
  "destination": {
    "type": "postgis",
    "schema": "raw_data",
    "mode": "create"
  }
}
```

`read_options` is optional: CSV accepts `sep`, `encoding`, `decimal`, `dtype`
and `header`; Excel accepts `dtype` and `header`. These options are passed to
pandas. By default pandas infers column types; specify `dtype` when identifiers
such as postal codes must remain strings. The destination modes are `create`
(fail if the table exists), `overwrite` (replace the table), and `append`
(require an existing compatible table). Each table is loaded transactionally.
`target_crs` applies only to geographic data. Ingestion history and reports use
the existing layer and feature terminology for sheets and rows.

## Geographic geometry types

Polygon shapefiles can contain both Polygon and MultiPolygon features, even
when OGR reports their layer type as Polygon. By default the loader promotes
polygon shapefile geometries to MultiPolygon, so PostGIS can store both types.
This applies equally to shapefiles extracted from archives. Point and line
shapefiles and other source formats retain their existing behavior.

Set `destination.options.promote_to_multi` to `true` to request OGR's
`PROMOTE_TO_MULTI` for other formats or geometry types, or to `false` to disable
automatic promotion. In append mode, the existing destination geometry column
must be compatible with the resulting type; promotion does not alter it.
Disabling promotion for mixed Polygon/MultiPolygon data can cause loading to
fail.

## ZIP and 7z archives

Local and HTTP sources can point to ZIP or 7z archives. Archives are detected
by their extension or contents. If an HTTP URL has no extension, optionally set
`source.options.filename` to a name ending in `.zip` or `.7z`. Formats such as
XLSX and KMZ are ZIP containers and are handled directly by their loaders.

An archive containing exactly one recognized dataset is selected automatically.
Recognized extensions are CSV, XLSX, XLS, SHP, GPKG, GeoJSON, JSON, GeoJSONL,
NDJSON, GML, KML, KMZ, FGB, SQLite and Parquet. Other files, including
shapefile sidecars, do not count as datasets. When there are several datasets,
or to select another format supported by OGR, specify the exact,
case-sensitive path within the archive:

```json
{
  "source": {
    "name": "communes",
    "type": "local",
    "location": "/path/to/data.zip",
    "options": {
      "archive_member": "geographic/communes.shp"
    }
  },
  "destination": {
    "type": "postgis",
    "schema": "raw_data",
    "mode": "create"
  }
}
```

Destination table names use `source.name`, just as for an uncompressed source.
All archive files are extracted together so associated
files such as `.dbf`, `.shx` and `.prj` remain available. Extraction uses a
private temporary directory, removed after ingestion even on failure. Unsafe
paths, links, special files, encrypted members and duplicate paths are rejected.
Archives are limited to 10,000 entries and 10 GiB of uncompressed data. Encrypted
archives are not supported. Nested archives are not unpacked recursively.

## Destination table names

For a source with one layer or sheet, the table name is the normalized
`source.name`; with multiple layers or sheets, each table is named
`source.name` followed by `_` and the layer or sheet name. Names are converted
to lowercase ASCII identifiers and limited to 63 characters; collisions cause
an error. This naming applies to local and HTTP sources, including archives.
It replaces the previous file-based naming: existing tables are not renamed or
deleted. In `append` and `overwrite` modes, verify the new table name before
running an existing configuration.

For example:

```sh
uv run python PATH/GeoDataLab/ingestion/run_ingestion.py --config /path/to/ingestion.json
```

The separate GeoDataLab ingestion project pins this library to a Git revision.
To use these changes in its own environment or Docker image, publish the new
revision, update that project's dependency revision and lock file, and rebuild
or reinstall its dependencies.
