import io
import zipfile

import pytest

from quantum_lake_student.stages.register_sources import (
    archive_member_is_safe,
    canonical_bronze_path,
    validate_zip_members,
)
from quantum_lake_student.models import stable_record_hash


def make_zip(member_names: list[str]) -> bytes:
    output = io.BytesIO()

    with zipfile.ZipFile(output, "w") as archive:
        for name in member_names:
            archive.writestr(name, b"test")

    return output.getvalue()


def test_safe_archive_member() -> None:
    assert archive_member_is_safe(
        "folder/data.csv"
    )


def test_parent_traversal_is_rejected() -> None:
    assert not archive_member_is_safe(
        "../secret.txt"
    )

    assert not archive_member_is_safe(
        "folder/../../secret.txt"
    )


def test_absolute_path_is_rejected() -> None:
    assert not archive_member_is_safe(
        "/absolute/data.csv"
    )


def test_windows_style_traversal_is_rejected() -> None:
    assert not archive_member_is_safe(
        r"..\secret.txt"
    )


def test_zip_with_unsafe_member_is_rejected() -> None:
    data = make_zip(
        [
            "safe.csv",
            "../escape.csv",
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="Unsafe archive member",
    ):
        validate_zip_members(
            data,
            "test.zip",
        )


def test_raw_path_maps_to_bronze() -> None:
    assert canonical_bronze_path(
        "raw/source=test/data.zip"
    ) == "bronze/source=test/data.zip"

def test_source_identifier_is_stable() -> None:
    value = {
        "source": "qasmbench",
        "bronze_object": "bronze/source=qasmbench/qasmbench-qec.zip",
        "sha256": (
            "60307f88e34b1f752b94223d6d136da72c629b4b625ac6d1c30e5e2e4f85722a"
        ),
    }

    first = stable_record_hash(value)
    second = stable_record_hash(value)

    assert first == second

def test_source_object_id_is_deterministic() -> None:
    value = {
        "source": "qasmbench",
        "bronze_object": "bronze/source=qasmbench/qasmbench-qec.zip",
        "sha256": (
            "60307f88e34b1f752b94223d6d136da72c629b4b625ac6d1c30e5e2e4f85722a"
        ),
    }

    first = "srcobj_" + stable_record_hash(value)[:24]
    second = "srcobj_" + stable_record_hash(value)[:24]

    assert first == second

def test_empty_archive_member_is_rejected() -> None:
    assert not archive_member_is_safe("")