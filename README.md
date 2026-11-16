# GeoDataLab Ingestion

Python library for geospatial and tabular data ingestion into PostgreSQL/PostGIS.

## Features

- Local and HTTP data sources
- Geospatial layer discovery
- OGR-based ingestion into PostGIS
- CSV and Excel (.xlsx and .xls) ingestion using pandas
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
table uses the normalized file stem, while multiple sheets use the normalized
file stem and sheet name. Name collisions are reported as errors.

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

For example:

```sh
uv run python PATH/GeoDataLab/ingestion/run_ingestion.py --config /path/to/ingestion.json
```

The separate GeoDataLab ingestion project pins this library to a Git revision.
To use these changes in its own environment or Docker image, publish the new
revision, update that project's dependency revision and lock file, and rebuild
or reinstall its dependencies.
