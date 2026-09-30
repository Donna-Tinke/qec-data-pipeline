"""Register and verify the original source files.

Bronze files are supplied by the course and must remain unchanged.
This stage verifies the release inputs and exposes deterministic source
metadata for downstream tracing and run records.
"""

from __future__ import annotations

import hashlib
import io
import zipfile
from dataclasses import dataclass
from pathlib import PurePosixPath

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import bronze_inventory, minio_client
from quantum_lake_student.models import StageResult, stable_record_hash


@dataclass(frozen=True)
class ExpectedSource:
    source: str
    bronze_object: str
    sha256: str
    bytes: int


EXPECTED_SOURCES = (
    ExpectedSource(
        source="qasmbench",
        bronze_object="bronze/source=qasmbench/qasmbench-qec.zip",
        sha256="60307f88e34b1f752b94223d6d136da72c629b4b625ac6d1c30e5e2e4f85722a",
        bytes=144_172,
    ),
    ExpectedSource(
        source="qec_syndromes",
        bronze_object="bronze/source=qec_syndromes/syndromes_dataset.zip",
        sha256="bdfce36a71f04295ac78fb372d9c2e381801c05e3be119f919750ef59026d072",
        bytes=358_017,
    ),
    ExpectedSource(
        source="google_qec",
        bronze_object="bronze/source=google_qec/google-surface-code-curated.zip",
        sha256="5d6a24f89f055883a49910979490be4baef54d28bf9a0f8e096a1d6c46d1ea56",
        bytes=14_638_673,
    ),
)


def canonical_bronze_path(path: str) -> str:
    """Treat the local release raw/ prefix as equivalent to bronze/."""
    if path.startswith("raw/"):
        return "bronze/" + path.removeprefix("raw/")
    return path


def sha256_bytes(data: bytes) -> str:
    """Return the SHA-256 hash of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def archive_member_is_safe(name: str) -> bool:
    """Reject archive paths that could escape the archive root."""
    if not name or "\\" in name:
        return False

    path = PurePosixPath(name)

    if path.is_absolute():
        return False

    if ".." in path.parts:
        return False

    return True


def validate_zip_members(data: bytes, object_name: str) -> list[str]:
    """Validate the ZIP and return its member names."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = archive.namelist()
    except zipfile.BadZipFile as exc:
        raise RuntimeError(
            f"{object_name} is not a valid ZIP archive"
        ) from exc

    unsafe = [
        member
        for member in members
        if not archive_member_is_safe(member)
    ]

    if unsafe:
        raise RuntimeError(
            f"Unsafe archive member path(s) in {object_name}: {unsafe}"
        )

    return members


def _inventory_lookup(
    settings: Settings,
) -> dict[str, tuple[str, int]]:
    """
    Map canonical Bronze path -> (actual stored path, size).

    On the Docker platform the path begins with bronze/.
    In local release mode it may begin with raw/.
    """
    result: dict[str, tuple[str, int]] = {}

    for stored_path, size in bronze_inventory(settings):
        canonical = canonical_bronze_path(stored_path)

        if canonical in result:
            raise RuntimeError(
                f"Duplicate source object for {canonical}"
            )

        result[canonical] = (stored_path, size)

    return result


def read_source_object(
    settings: Settings,
    stored_path: str,
) -> bytes:
    """Read one source object without modifying it."""
    if settings.lake_backend == "local":
        return (
            settings.local_lake_root / stored_path
        ).read_bytes()

    client = minio_client(settings)
    response = client.get_object(
        settings.s3_bucket,
        stored_path,
    )

    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def verified_source_metadata(
    settings: Settings,
) -> list[dict[str, object]]:
    """
    Verify all three inputs and return deterministic metadata.

    This metadata can later be reused in run.json and source tracing.
    """
    inventory = _inventory_lookup(settings)

    expected_paths = {
        source.bronze_object
        for source in EXPECTED_SOURCES
    }

    actual_paths = set(inventory)

    missing = expected_paths - actual_paths
    unexpected = actual_paths - expected_paths

    if missing:
        raise RuntimeError(
            f"Missing required Bronze object(s): {sorted(missing)}"
        )

    if unexpected:
        raise RuntimeError(
            f"Unexpected Bronze object(s): {sorted(unexpected)}"
        )

    rows: list[dict[str, object]] = []

    for expected in EXPECTED_SOURCES:
        stored_path, observed_size = inventory[
            expected.bronze_object
        ]

        if observed_size != expected.bytes:
            raise RuntimeError(
                f"Byte-size mismatch for "
                f"{expected.bronze_object}: "
                f"expected {expected.bytes}, "
                f"got {observed_size}"
            )

        data = read_source_object(
            settings,
            stored_path,
        )

        actual_hash = sha256_bytes(data)

        if actual_hash != expected.sha256:
            raise RuntimeError(
                f"SHA-256 mismatch for "
                f"{expected.bronze_object}: "
                f"expected {expected.sha256}, "
                f"got {actual_hash}"
            )

        members = validate_zip_members(
            data,
            expected.bronze_object,
        )

        source_object_id = (
            "srcobj_"
            + stable_record_hash(
                {
                    "source": expected.source,
                    "bronze_object": expected.bronze_object,
                    "sha256": actual_hash,
                }
            )[:24]
        )

        rows.append(
            {
                "source": expected.source,
                "source_object_id": source_object_id,
                "bronze_object": expected.bronze_object,
                "input_sha256": actual_hash,
                "bytes": observed_size,
                "archive_member_count": len(members),
                "archive_members": tuple(members),
            }
        )

    return rows


def bronze_run_facts(
    settings: Settings,
) -> dict[str, object]:
    """Return Bronze metadata for the Part I run record."""
    sources = verified_source_metadata(settings)

    return {
        "bronze_input_count": len(sources),
        "bronze_inputs": [
            {
                "source": source["source"],
                "source_object_id": source["source_object_id"],
                "bronze_object": source["bronze_object"],
                "input_sha256": source["input_sha256"],
                "bytes": source["bytes"],
                "archive_member_count": source["archive_member_count"],
            }
            for source in sources
        ],
    }


def run(run_id: str) -> StageResult:
    result = StageResult(
        stage="register_sources",
        run_id=run_id,
    )

    settings = Settings.from_environment()

    sources = verified_source_metadata(settings)

    result.input_count = len(sources)
    result.output_count = len(sources)

    result.finish()

    return result