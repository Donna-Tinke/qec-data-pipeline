import zipfile
from pathlib import Path

import pytest

from quantum_lake_student.archives import extract_archive, safe_member_names


def _make_zip(path: Path, names: list[str]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name in names:
            archive.writestr(name, "content")
    return path


def test_safe_member_names_accepts_normal_paths(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path / "good.zip", ["experiment_1/properties.yml", "README.txt"])
    with zipfile.ZipFile(zip_path) as archive:
        assert safe_member_names(archive) == ["experiment_1/properties.yml", "README.txt"]


def test_safe_member_names_rejects_parent_traversal(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path / "evil.zip", ["../../etc/passwd"])
    with zipfile.ZipFile(zip_path) as archive:
        with pytest.raises(ValueError):
            safe_member_names(archive)


def test_safe_member_names_rejects_absolute_paths(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path / "evil.zip", ["/etc/passwd"])
    with zipfile.ZipFile(zip_path) as archive:
        with pytest.raises(ValueError):
            safe_member_names(archive)


def test_extract_archive_writes_only_into_destination(tmp_path: Path) -> None:
    zip_path = _make_zip(
        tmp_path / "bundle.zip", ["experiment_1/properties.yml", "experiment_1/measurements.b8"]
    )
    destination = tmp_path / "scratch"

    extract_archive(zip_path, destination)

    assert (destination / "experiment_1" / "properties.yml").read_text() == "content"
    assert (destination / "experiment_1" / "measurements.b8").read_text() == "content"


def test_extract_archive_refuses_unsafe_member(tmp_path: Path) -> None:
    zip_path = _make_zip(tmp_path / "evil.zip", ["../outside.txt"])
    destination = tmp_path / "scratch"

    with pytest.raises(ValueError):
        extract_archive(zip_path, destination)

    assert not (tmp_path / "outside.txt").exists()
