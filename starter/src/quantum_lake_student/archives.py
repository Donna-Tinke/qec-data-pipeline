"""Zip helpers for reading the Bronze archives without touching Bronze itself.

google_qec and qasmbench both come as zip files, so this is shared between them.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path, PurePosixPath
from typing import BinaryIO


def safe_member_names(archive: zipfile.ZipFile) -> list[str]:
    # blocks zip-slip: a member path trying to escape the extract folder (../.. etc)
    names = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        name = info.filename
        pure = PurePosixPath(name)
        if name.startswith("/") or name.startswith("\\") or pure.is_absolute() or ".." in pure.parts:
            raise ValueError(f"unsafe archive member path: {name!r}")
        names.append(name)
    return names


def extract_archive(
    zip_source: Path | BinaryIO | bytes, destination: Path
) -> Path:
    # only reads zip_source, never writes to it - Bronze stays untouched
    destination.mkdir(parents=True, exist_ok=True)
    source = io.BytesIO(zip_source) if isinstance(zip_source, bytes) else zip_source
    with zipfile.ZipFile(source) as archive:
        names = safe_member_names(archive)
        archive.extractall(destination, members=names)
    return destination
