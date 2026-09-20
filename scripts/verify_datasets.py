#!/usr/bin/env python3
"""Verify the student data release and, when requested, upstream archives."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
import zipfile
from pathlib import Path, PurePosixPath


INFRASTRUCTURE = Path(__file__).resolve().parents[1]
DATASETS = INFRASTRUCTURE / "datasets"

EXPECTED_QASM_BENCHMARKS = {
    "error_correctiond3_n5",
    "qec_en_n5",
    "qec_sm_n5",
}
EXPECTED_GOOGLE_EXPERIMENTS = {
    "surface_code_bX_d3_r25_center_3_5",
    "surface_code_bX_d3_r25_center_5_3",
    "surface_code_bX_d3_r25_center_5_7",
    "surface_code_bX_d3_r25_center_7_5",
    "surface_code_bX_d5_r25_center_5_5",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_archive(path: Path) -> None:
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            invalid_member = archive.testzip()
            if invalid_member:
                raise AssertionError(f"Corrupt ZIP member {invalid_member} in {path}")
        return
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getmembers()
            if not members:
                raise AssertionError(f"Empty TAR archive: {path}")
            unsafe = [
                member.name
                for member in members
                if PurePosixPath(member.name).is_absolute()
                or ".." in PurePosixPath(member.name).parts
            ]
            if unsafe:
                raise AssertionError(f"Unsafe TAR member paths in {path}: {unsafe[:3]}")


def verify_checksum_file(*, required: bool) -> None:
    checksum_path = DATASETS / "CHECKSUMS.sha256"
    if not checksum_path.is_file():
        if required:
            raise AssertionError(
                "Instructor upstream checksum file is missing from this copy."
            )
        print("SKIP upstream archives (not included in the student release)")
        return
    for line_number, line in enumerate(
        checksum_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        expected, relative = line.split(maxsplit=1)
        path = DATASETS / relative
        if not path.is_file():
            raise AssertionError(f"Missing file listed on line {line_number}: {path}")
        actual = sha256_file(path)
        if actual != expected:
            raise AssertionError(
                f"Checksum mismatch for {path}: expected {expected}, got {actual}"
            )
        verify_archive(path)
        print(f"OK upstream {relative}")


def verify_qec_release_contents(core: Path) -> None:
    qasm_path = core / "raw/source=qasmbench/qasmbench-qec.zip"
    with zipfile.ZipFile(qasm_path) as archive:
        qasm_benchmarks = {
            Path(name).parent.name
            for name in archive.namelist()
            if name.endswith(".qasm") and name.startswith("small/")
        }
    if qasm_benchmarks != EXPECTED_QASM_BENCHMARKS:
        raise AssertionError(
            "QASMBench subset differs from the three approved QEC benchmarks: "
            f"{sorted(qasm_benchmarks)}"
        )

    syndromes_path = core / "raw/source=qec_syndromes/syndromes_dataset.zip"
    with zipfile.ZipFile(syndromes_path) as archive:
        csv_members = [name for name in archive.namelist() if name.endswith(".csv")]
        if len(csv_members) != 7:
            raise AssertionError(
                f"Expected seven syndrome CSV files, found {len(csv_members)}"
            )
        for member in csv_members:
            header = archive.read(member).splitlines()[0].decode("utf-8")
            if header != "labels,syndromes,quantity":
                raise AssertionError(f"Unexpected syndrome header in {member}: {header}")

    google_path = core / "raw/source=google_qec/google-surface-code-curated.zip"
    with zipfile.ZipFile(google_path) as archive:
        experiments = {
            name.split("/", 1)[0]
            for name in archive.namelist()
            if "/" in name and name.startswith("surface_code_")
        }
        if experiments != EXPECTED_GOOGLE_EXPERIMENTS:
            raise AssertionError(
                "Google QEC subset differs from the approved five experiments: "
                f"{sorted(experiments)}"
            )
        required_files = {
            "properties.yml",
            "measurements.b8",
            "sweep.b8",
            "detection_events.b8",
            "obs_flips_actual.01",
        }
        names = set(archive.namelist())
        for experiment in experiments:
            missing = {
                filename
                for filename in required_files
                if f"{experiment}/{filename}" not in names
            }
            if missing:
                raise AssertionError(
                    f"Google experiment {experiment} misses {sorted(missing)}"
                )

def verify_student_bundle() -> None:
    bundle_dir = DATASETS / "student-bundle"
    core = bundle_dir / "core"
    manifest_path = core / "metadata" / "bundle-manifest.json"
    if not manifest_path.is_file():
        raise AssertionError(
            "Student bundle has not been built. Run `make data-bundle` first."
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("bundle_version") != 3:
        raise AssertionError("Expected two-part QEC bundle version 3")
    mandatory_sources = {
        item["source"] for item in manifest["objects"] if item.get("mandatory", True)
    }
    expected_sources = {
        "qasmbench",
        "qec_syndromes",
        "google_qec",
    }
    if mandatory_sources != expected_sources:
        raise AssertionError(
            f"Unexpected mandatory source set: {sorted(mandatory_sources)}"
        )
    if len(manifest["objects"]) != 3:
        raise AssertionError("The streamlined release must contain exactly three objects")
    for item in manifest["objects"]:
        path = core / item["path"]
        if not path.is_file():
            raise AssertionError(f"Missing bundle object {path}")
        if path.stat().st_size != item["bytes"]:
            raise AssertionError(f"Size mismatch for {path}")
        if sha256_file(path) != item["sha256"]:
            raise AssertionError(f"Checksum mismatch for {path}")
        verify_archive(path)
        print(f"OK bundle object {item['path']}")

    verify_qec_release_contents(core)
    print("OK QEC-specific release structure")

    release_name = manifest["release_name"]
    release_zip = bundle_dir / f"{release_name}.zip"
    checksum_file = bundle_dir / f"{release_name}.zip.sha256"
    if release_zip.is_file() and checksum_file.is_file():
        expected = checksum_file.read_text(encoding="utf-8").split()[0]
        if sha256_file(release_zip) != expected:
            raise AssertionError(f"Release checksum mismatch for {release_zip}")
        with zipfile.ZipFile(release_zip) as archive:
            invalid_member = archive.testzip()
            if invalid_member:
                raise AssertionError(f"Corrupt release member {invalid_member}")
        print(f"OK release {release_zip.name}")
    else:
        print("SKIP standalone data ZIP (unpacked core release is present)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--require-upstream",
        action="store_true",
        help="fail unless all instructor upstream archives are present and valid",
    )
    args = parser.parse_args()
    verify_checksum_file(required=args.require_upstream)
    verify_student_bundle()
    print("All dataset integrity checks passed.")


if __name__ == "__main__":
    main()
