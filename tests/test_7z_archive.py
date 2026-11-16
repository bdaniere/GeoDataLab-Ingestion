from datetime import UTC, datetime
from pathlib import Path

import py7zr
import pytest
from sqlalchemy import text

from geodatalab_ingestion.core.ingestion_runner import IngestionRunner
from geodatalab_ingestion.core.models import DestinationConfig, IngestionResult, SourceData
from geodatalab_ingestion.extraction.archive_manager import ArchiveManager
from geodatalab_ingestion.extraction.layer_discovery import LayerDiscovery


def make_7z(path, files):
    with py7zr.SevenZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(data, name)
    return path


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_7z_csv_ingestion(database, tmp_path, tabular_source, source_type):
    archive_path = tmp_path / "data.7z"
    with py7zr.SevenZipFile(archive_path, "w") as archive:
        archive.writestr("id;nom\n1;Lyon\n2;Paris\n", "nested/population.csv")
    source = tabular_source(
        archive_path, source_type, {"read_options": {"sep": ";"}}
    )
    result = IngestionRunner().run(source, DestinationConfig("postgis", "main"))
    assert result.success
    with database.connect() as connection:
        assert connection.execute(text("SELECT * FROM data ORDER BY id")).all() == [
            (1, "Lyon"), (2, "Paris")
        ]


@pytest.mark.parametrize("source_type", ["local", "http"])
def test_7z_select_member_and_discover_shapefile(
    tmp_path, tabular_source, source_type, monkeypatch, ogrinfo
):
    shapefile_dir = Path(__file__).parent / "testdata" / "test_shapefile"
    files = {
        f"nested/{p.name}": p.read_bytes()
        for p in shapefile_dir.iterdir()
        if p.is_file()
    }
    files["nested/list.csv"] = b"id\n1\n"
    archive_path = make_7z(tmp_path / "data.7z", files)
    source = tabular_source(
        archive_path, source_type, {"archive_member": "nested/test.shp"}
    )
    seen = []

    class Loader:
        def load(self, source, destination):
            path = Path(source.content)
            assert path.with_suffix(".dbf").is_file()
            assert path.with_suffix(".shx").is_file()
            assert source.metadata["archive_member"] == "nested/test.shp"
            assert destination.table == "data"
            seen.append(path)
            now = datetime.now(UTC)
            return IngestionResult(source.source_name, True, now, now)

    monkeypatch.setattr(IngestionRunner, "_initialize_history", lambda self: None)
    result = IngestionRunner(loader=Loader(), layer_discovery=LayerDiscovery()).run(
        source, DestinationConfig("postgis")
    )
    assert result.success
    assert list(result.layer_results) == ["test"]
    assert seen and not seen[0].exists()


def test_7z_detected_by_content_and_cleans_up_on_error(tmp_path):
    archive = make_7z(tmp_path / "download", {"nested/data.csv": "id\n1\n"})
    with pytest.raises(RuntimeError, match="failure"):
        with ArchiveManager().prepare(SourceData("test", "http", archive)) as prepared:
            path = prepared.content
            assert prepared.format == "csv"
            assert path.read_text() == "id\n1\n"
            raise RuntimeError("failure")
    assert not path.exists()


@pytest.mark.parametrize(
    "files,member,message",
    [
        ({"one.csv": "x", "two.csv": "y"}, None, "exactly one"),
        ({"notes.txt": "x"}, None, "exactly one"),
        ({"one.csv": "x"}, "missing.csv", "not found"),
        ({"one.csv": "x"}, "", "non-empty"),
        ({"one.csv": "x"}, 1, "non-empty"),
    ],
)
def test_7z_selection_errors(tmp_path, files, member, message):
    archive = make_7z(tmp_path / "data.7z", files)
    source = SourceData("test", "local", archive, "7z", {"archive_member": member})
    with pytest.raises(ValueError, match=message):
        with ArchiveManager().prepare(source):
            pass


@pytest.mark.parametrize(
    "name",
    ["../escape.csv", "/absolute.csv", "dir/../../escape.csv", "dir\\escape.csv",
     "C:/escape.csv", "dir/./file.csv", "dir//file.csv"],
)
def test_7z_rejects_unsafe_paths(tmp_path, monkeypatch, name):
    archive = make_7z(tmp_path / "data.7z", {"safe.csv": "data", "other.txt": "data"})
    original_list = py7zr.SevenZipFile.list

    def unsafe_list(self):
        entries = original_list(self)
        entries[1].filename = name
        return entries

    monkeypatch.setattr(py7zr.SevenZipFile, "list", unsafe_list)
    source = SourceData("test", "local", archive, "7z", {"archive_member": "safe.csv"})
    with pytest.raises(ValueError, match="Unsafe 7z member path"):
        with ArchiveManager().prepare(source):
            pass
    assert not (tmp_path / "escape.csv").exists()


def test_7z_rejects_symlinks_duplicates_and_encrypted_archives(tmp_path):
    link = tmp_path / "link"
    target = tmp_path / "target"
    target.write_text("data")
    link.symlink_to(target)
    path = tmp_path / "data.7z"
    with py7zr.SevenZipFile(path, "w") as archive:
        archive.write(target, "safe.csv")
        archive.write(link, "link")
    with pytest.raises(ValueError, match="Unsupported 7z member type"):
        with ArchiveManager().prepare(SourceData("test", "local", path, "7z")):
            pass

    with py7zr.SevenZipFile(path, "w") as archive:
        archive.writestr("first", "safe.csv")
        archive.writestr("second", "safe.csv")
    with pytest.raises(ValueError, match="Duplicate 7z member"):
        with ArchiveManager().prepare(SourceData("test", "local", path, "7z")):
            pass

    with py7zr.SevenZipFile(path, "w", password="password") as archive:
        archive.writestr("data", "safe.csv")
    with pytest.raises(ValueError, match="Encrypted 7z archives"):
        with ArchiveManager().prepare(SourceData("test", "local", path, "7z")):
            pass


def test_7z_limits_and_corrupt_archive(tmp_path, monkeypatch):
    path = make_7z(tmp_path / "data.7z", {"data.csv": "id\n1\n"})
    source = SourceData("test", "local", path, "7z")
    monkeypatch.setattr(
        "geodatalab_ingestion.extraction.archive_manager.MAX_UNCOMPRESSED_SIZE", 3
    )
    with pytest.raises(ValueError, match="maximum uncompressed size"):
        with ArchiveManager().prepare(source):
            pass
    monkeypatch.setattr(
        "geodatalab_ingestion.extraction.archive_manager.MAX_UNCOMPRESSED_SIZE", 100
    )
    monkeypatch.setattr(
        "geodatalab_ingestion.extraction.archive_manager.MAX_ARCHIVE_FILES", 0
    )
    with pytest.raises(ValueError, match="more than 0 entries"):
        with ArchiveManager().prepare(source):
            pass
    path.write_bytes(b"not an archive")
    with pytest.raises(py7zr.Bad7zFile):
        with ArchiveManager().prepare(source):
            pass
