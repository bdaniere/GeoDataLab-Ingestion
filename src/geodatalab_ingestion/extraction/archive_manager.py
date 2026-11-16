import shutil
import stat
import tempfile
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path, PurePosixPath, PureWindowsPath

from geodatalab_ingestion.core.models import SourceData

MAX_ARCHIVE_FILES = 10_000
MAX_UNCOMPRESSED_SIZE = 10 * 1024**3
DATASET_EXTENSIONS = {
    ".csv",
    ".xlsx",
    ".xls",
    ".shp",
    ".gpkg",
    ".geojson",
    ".json",
    ".geojsonl",
    ".ndjson",
    ".gml",
    ".kml",
    ".kmz",
    ".fgb",
    ".sqlite",
    ".parquet",
}


class ArchiveManager:
    """Prepare ZIP datasets in a private temporary directory."""

    @contextmanager
    def prepare(self, source: SourceData) -> Generator[SourceData]:
        path = Path(source.content)
        source_format = (source.format or path.suffix.lstrip(".")).lower()
        member = source.metadata.get("archive_member")
        # XLSX and KMZ are ZIP containers handled directly by their loaders.
        is_archive = source_format == "zip" or (
            source_format not in {"xlsx", "kmz"} and zipfile.is_zipfile(path)
        )
        if not is_archive:
            if member is not None:
                raise ValueError("archive_member requires a ZIP source.")
            yield source
            return

        with (
            zipfile.ZipFile(path) as archive,
            tempfile.TemporaryDirectory(prefix="geodatalab-archive-") as directory,
        ):
            entries = self._validate_entries(archive)
            selected = self._select_member(entries, member)
            root = Path(directory)
            for entry in entries:
                target = root.joinpath(*PurePosixPath(entry.filename).parts)
                if entry.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with (
                    archive.open(entry) as input_file,
                    target.open("xb") as output_file,
                ):
                    shutil.copyfileobj(input_file, output_file)

            selected_path = root.joinpath(*PurePosixPath(selected.filename).parts)
            yield replace(
                source,
                content=selected_path,
                format=selected_path.suffix.lstrip(".").lower() or None,
                metadata={
                    **source.metadata,
                    "archive_path": str(path),
                    "archive_member": selected.filename,
                },
            )

    @staticmethod
    def _validate_entries(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
        entries = archive.infolist()
        if len(entries) > MAX_ARCHIVE_FILES:
            raise ValueError(f"ZIP contains more than {MAX_ARCHIVE_FILES} entries.")
        if sum(entry.file_size for entry in entries) > MAX_UNCOMPRESSED_SIZE:
            raise ValueError("ZIP exceeds the maximum uncompressed size of 10 GiB.")

        seen: set[str] = set()
        for entry in entries:
            name = entry.filename
            parts = name.rstrip("/").split("/")
            mode = entry.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            if (
                not name
                or "\\" in name
                or PurePosixPath(name).is_absolute()
                or PureWindowsPath(name).drive
                or any(part in {"", ".", ".."} for part in parts)
            ):
                raise ValueError(f"Unsafe ZIP member path: {name!r}")
            if file_type not in {0, stat.S_IFREG, stat.S_IFDIR} or (
                file_type == stat.S_IFDIR and not entry.is_dir()
            ):
                raise ValueError(f"Unsupported ZIP member type: {name!r}")
            if entry.flag_bits & 1:
                raise ValueError(f"Encrypted ZIP member is not supported: {name!r}")
            normalized = "/".join(parts)
            if normalized in seen:
                raise ValueError(f"Duplicate ZIP member: {name!r}")
            seen.add(normalized)
        return entries

    @staticmethod
    def _select_member(
        entries: list[zipfile.ZipInfo], member: object
    ) -> zipfile.ZipInfo:
        files = [entry for entry in entries if not entry.is_dir()]
        if member is not None:
            if not isinstance(member, str) or not member:
                raise ValueError("archive_member must be a non-empty ZIP member path.")
            matches = [entry for entry in files if entry.filename == member]
            if not matches:
                raise ValueError(f"ZIP member not found: {member!r}")
            return matches[0]

        candidates = [
            entry
            for entry in files
            if PurePosixPath(entry.filename).suffix.lower() in DATASET_EXTENSIONS
        ]
        if len(candidates) != 1:
            names = ", ".join(entry.filename for entry in candidates) or "none"
            raise ValueError(
                "ZIP must contain exactly one recognized dataset; "
                f"found: {names}. Set source.options.archive_member."
            )
        return candidates[0]
