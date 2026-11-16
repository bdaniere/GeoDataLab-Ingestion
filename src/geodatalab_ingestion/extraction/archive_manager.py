import shutil
import stat
import tempfile
import zipfile
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath, PureWindowsPath

import py7zr

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


@dataclass(frozen=True)
class ArchiveEntry:
    filename: str
    is_directory: bool
    size: int


class ArchiveManager:
    """Prepare ZIP and 7z datasets in a private temporary directory."""

    @contextmanager
    def prepare(self, source: SourceData) -> Generator[SourceData]:
        path = Path(source.content)
        source_format = (source.format or path.suffix.lstrip(".")).lower()
        member = source.metadata.get("archive_member")
        # XLSX and KMZ are ZIP containers handled directly by their loaders.
        archive_format = None
        if source_format == "7z" or py7zr.is_7zfile(path):
            archive_format = "7z"
        elif source_format == "zip" or (
            source_format not in {"xlsx", "kmz"} and zipfile.is_zipfile(path)
        ):
            archive_format = "ZIP"
        if archive_format is None:
            if member is not None:
                raise ValueError("archive_member requires a ZIP or 7z source.")
            yield source
            return

        with ExitStack() as stack:
            directory = stack.enter_context(
                tempfile.TemporaryDirectory(prefix="geodatalab-archive-")
            )
            if archive_format == "ZIP":
                archive = stack.enter_context(zipfile.ZipFile(path))
                entries = self._zip_entries(archive)
            else:
                archive = stack.enter_context(
                    py7zr.SevenZipFile(path, max_extract_size=MAX_UNCOMPRESSED_SIZE)
                )
                entries = self._seven_entries(archive)
            self._validate_entries(entries, archive_format)
            selected = self._select_member(entries, member, archive_format)
            root = Path(directory)
            if isinstance(archive, zipfile.ZipFile):
                for entry in archive.infolist():
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
            else:
                archive.extractall(path=root)

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
    def _zip_entries(archive: zipfile.ZipFile) -> list[ArchiveEntry]:
        entries = []
        for entry in archive.infolist():
            mode = entry.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            if file_type not in {0, stat.S_IFREG, stat.S_IFDIR} or (
                file_type == stat.S_IFDIR and not entry.is_dir()
            ):
                raise ValueError(f"Unsupported ZIP member type: {entry.filename!r}")
            if entry.flag_bits & 1:
                raise ValueError(f"Encrypted ZIP member is not supported: {entry.filename!r}")
            entries.append(ArchiveEntry(entry.filename, entry.is_dir(), entry.file_size))
        return entries

    @staticmethod
    def _seven_entries(archive: py7zr.SevenZipFile) -> list[ArchiveEntry]:
        if archive.needs_password():
            raise ValueError("Encrypted 7z archives are not supported.")
        entries = []
        for entry in archive.list():
            if entry.is_symlink or not (entry.is_file or entry.is_directory):
                raise ValueError(f"Unsupported 7z member type: {entry.filename!r}")
            entries.append(
                ArchiveEntry(entry.filename, entry.is_directory, entry.uncompressed)
            )
        return entries

    @staticmethod
    def _validate_entries(entries: list[ArchiveEntry], archive_format: str) -> None:
        if len(entries) > MAX_ARCHIVE_FILES:
            raise ValueError(f"{archive_format} contains more than {MAX_ARCHIVE_FILES} entries.")
        if sum(entry.size for entry in entries) > MAX_UNCOMPRESSED_SIZE:
            raise ValueError(
                f"{archive_format} exceeds the maximum uncompressed size of 10 GiB."
            )

        seen: set[str] = set()
        for entry in entries:
            name = entry.filename
            parts = name.rstrip("/").split("/")
            if (
                not name
                or "\\" in name
                or PurePosixPath(name).is_absolute()
                or PureWindowsPath(name).drive
                or any(part in {"", ".", ".."} for part in parts)
            ):
                raise ValueError(f"Unsafe {archive_format} member path: {name!r}")
            normalized = "/".join(parts)
            if normalized in seen:
                raise ValueError(f"Duplicate {archive_format} member: {name!r}")
            seen.add(normalized)

    @staticmethod
    def _select_member(
        entries: list[ArchiveEntry], member: object, archive_format: str
    ) -> ArchiveEntry:
        files = [entry for entry in entries if not entry.is_directory]
        if member is not None:
            if not isinstance(member, str) or not member:
                raise ValueError("archive_member must be a non-empty archive member path.")
            matches = [entry for entry in files if entry.filename == member]
            if not matches:
                raise ValueError(f"{archive_format} member not found: {member!r}")
            return matches[0]

        candidates = [
            entry
            for entry in files
            if PurePosixPath(entry.filename).suffix.lower() in DATASET_EXTENSIONS
        ]
        if len(candidates) != 1:
            names = ", ".join(entry.filename for entry in candidates) or "none"
            raise ValueError(
                f"{archive_format} must contain exactly one recognized dataset; "
                f"found: {names}. Set source.options.archive_member."
            )
        return candidates[0]
