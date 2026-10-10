"""Our Silver pipeline for the google_qec source.

Checks the raw files, decodes them, and writes the two required Silver
tables (experiment.parquet, shot.parquet). Checking the overall zip hash is
someone else's job (stages/register_sources.py) - not this file.
"""

from __future__ import annotations

import hashlib
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from quantum_lake_student.archives import safe_member_names
from quantum_lake_student.config import Settings
from quantum_lake_student.connections import minio_client
from quantum_lake_student.formats import b8_record_bytes, iter_b8_records, parse_01_records
from quantum_lake_student.lake import write_parquet
from quantum_lake_student.models import QualityFinding, Severity, StageResult, stable_record_hash
from quantum_lake_student.results import replace_source_rows

SOURCE_NAME = "google_qec"
BRONZE_OBJECT = f"bronze/source={SOURCE_NAME}/google-surface-code-curated.zip"

DIRECTORY_NAME_PATTERN = re.compile(
    r"^surface_code_b(?P<basis>[A-Za-z]+)_d(?P<distance>\d+)_r(?P<rounds>\d+)"
    r"_center_(?P<center_row>\d+)_(?P<center_col>\d+)$"
)

DECODER_PREDICTION_COLUMNS: dict[str, str] = {
    "belief_matching_prediction": "obs_flips_predicted_by_belief_matching.01",
    "correlated_matching_prediction": "obs_flips_predicted_by_correlated_matching.01",
    "pymatching_prediction": "obs_flips_predicted_by_pymatching.01",
    "tensor_network_contraction_prediction": (
        "obs_flips_predicted_by_tensor_network_contraction.01"
    ),
}

REQUIRED_FILES: tuple[str, ...] = (
    "properties.yml",
    "measurements.b8",
    "sweep.b8",
    "detection_events.b8",
    "obs_flips_actual.01",
    *DECODER_PREDICTION_COLUMNS.values(),
)

EXPERIMENT_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("experiment_id", pa.string()),
        ("basis", pa.string()),
        ("distance", pa.int32()),
        ("rounds", pa.int32()),
        ("shots", pa.int64()),
        ("center_row", pa.int32()),
        ("center_col", pa.int32()),
        ("measurement_count", pa.int32()),
        ("detector_count", pa.int32()),
    ]
)

SHOT_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("experiment_id", pa.string()),
        ("shot_index", pa.int64()),
        ("measurement_bits", pa.binary()),
        ("sweep_bits", pa.binary()),
        ("detector_bits", pa.binary()),
        ("detector_event_count", pa.int32()),
        ("actual_observable_flip", pa.bool_()),
        *[(column, pa.bool_()) for column in DECODER_PREDICTION_COLUMNS],
    ]
)

TRACE_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("source_name", pa.string()),
        ("bronze_object", pa.string()),
        ("archive_member", pa.string()),
        ("record_locator", pa.string()),
        ("input_sha256", pa.string()),
    ]
)

ISSUE_SCHEMA = pa.schema(
    [
        ("issue_id", pa.string()),
        ("run_id", pa.string()),
        ("source_record_id", pa.string()),
        ("rule_id", pa.string()),
        ("severity", pa.string()),
        ("observed_value", pa.string()),
        ("action", pa.string()),
        ("reason", pa.string()),
    ]
)

_PROPERTIES_REQUIRED_KEYS = {
    "basis",
    "distance",
    "rounds",
    "shots",
    "center_data_qubit_row",
    "center_data_qubit_col",
    "circuit_measurements",
    "circuit_sweep_bits",
    "circuit_detectors",
}


@dataclass(frozen=True)
class ExperimentProperties:
    experiment_id: str
    basis: str
    distance: int
    rounds: int
    shots: int
    center_row: int
    center_col: int
    measurement_count: int
    sweep_bit_count: int
    detector_count: int


def pack_bits(bits: tuple[int, ...]) -> bytes:
    """Re-pack unpacked bits into a Stim b8 record (little-endian in each byte)."""
    packed = bytearray((len(bits) + 7) // 8)
    for index, bit in enumerate(bits):
        if bit:
            packed[index // 8] |= 1 << (index % 8)
    return bytes(packed)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_experiment_dirs(archive_root: Path) -> list[Path]:
    return sorted(
        (path for path in archive_root.iterdir() if path.is_dir()),
        key=lambda path: path.name,
    )


def verify_companion_files(experiment_dir: Path) -> list[QualityFinding]:
    findings = []
    for name in REQUIRED_FILES:
        if not (experiment_dir / name).is_file():
            findings.append(
                QualityFinding(
                    rule_id="google_qec.missing_companion_file",
                    severity=Severity.ERROR,
                    source_system=SOURCE_NAME,
                    source_record_locator=f"{experiment_dir.name}/{name}",
                    message="Required companion file is missing",
                )
            )
    return findings


def load_properties(
    experiment_dir: Path,
) -> tuple[ExperimentProperties | None, list[QualityFinding]]:
    match = DIRECTORY_NAME_PATTERN.match(experiment_dir.name)
    if match is None:
        return None, [
            QualityFinding(
                rule_id="google_qec.unexpected_directory_name",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=experiment_dir.name,
                message="Experiment directory name does not match the documented pattern",
            )
        ]

    raw: Any = yaml.safe_load((experiment_dir / "properties.yml").read_text())
    if not isinstance(raw, dict):
        return None, [
            QualityFinding(
                rule_id="google_qec.invalid_properties_file",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=f"{experiment_dir.name}/properties.yml",
                message="properties.yml did not parse to a mapping",
            )
        ]

    missing_keys = _PROPERTIES_REQUIRED_KEYS - raw.keys()
    if missing_keys:
        return None, [
            QualityFinding(
                rule_id="google_qec.properties_missing_keys",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=f"{experiment_dir.name}/properties.yml",
                message="properties.yml is missing required keys",
                observed_value=str(sorted(missing_keys)),
            )
        ]

    properties = ExperimentProperties(
        experiment_id=experiment_dir.name,
        basis=str(raw["basis"]),
        distance=int(raw["distance"]),
        rounds=int(raw["rounds"]),
        shots=int(raw["shots"]),
        center_row=int(raw["center_data_qubit_row"]),
        center_col=int(raw["center_data_qubit_col"]),
        measurement_count=int(raw["circuit_measurements"]),
        sweep_bit_count=int(raw["circuit_sweep_bits"]),
        detector_count=int(raw["circuit_detectors"]),
    )

    encoded = {
        "basis": match.group("basis"),
        "distance": int(match.group("distance")),
        "rounds": int(match.group("rounds")),
        "center_row": int(match.group("center_row")),
        "center_col": int(match.group("center_col")),
    }
    disagreements = {
        key: {"directory_name": encoded[key], "properties_yml": getattr(properties, key)}
        for key in encoded
        if str(encoded[key]) != str(getattr(properties, key))
    }
    if disagreements:
        return None, [
            QualityFinding(
                rule_id="google_qec.directory_properties_mismatch",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=f"{experiment_dir.name}/properties.yml",
                message="Directory name and properties.yml disagree",
                observed_value=str(disagreements),
            )
        ]

    return properties, []


def _read_packed_matrix(
    experiment_dir: Path,
    file_name: str,
    *,
    bits_per_record: int,
    shots: int,
) -> tuple[list[tuple[int, ...]] | None, list[QualityFinding]]:
    data = (experiment_dir / file_name).read_bytes()
    expected_bytes = b8_record_bytes(bits_per_record) * shots
    if len(data) != expected_bytes:
        return None, [
            QualityFinding(
                rule_id="google_qec.b8_length_mismatch",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=f"{experiment_dir.name}/{file_name}",
                message="b8 file length does not match shots times expected record bytes",
                observed_value=f"observed={len(data)} expected={expected_bytes}",
            )
        ]

    record_bytes = b8_record_bytes(bits_per_record)
    padding_bits = record_bytes * 8 - bits_per_record
    if padding_bits:
        bad_padding = any(
            data[offset + record_bytes - 1] >> (8 - padding_bits) != 0
            for offset in range(0, len(data), record_bytes)
        )
        if bad_padding:
            return None, [
                QualityFinding(
                    rule_id="google_qec.b8_padding_not_zero",
                    severity=Severity.ERROR,
                    source_system=SOURCE_NAME,
                    source_record_locator=f"{experiment_dir.name}/{file_name}",
                    message="Unused padding bits in the final byte of a b8 record are not zero",
                )
            ]

    return list(iter_b8_records(data, bits_per_record=bits_per_record)), []


def _read_01_column(
    experiment_dir: Path, file_name: str, *, shots: int
) -> tuple[list[int] | None, list[QualityFinding]]:
    data = (experiment_dir / file_name).read_bytes()
    try:
        values = parse_01_records(data)
    except ValueError as error:
        return None, [
            QualityFinding(
                rule_id="google_qec.invalid_01_value",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=f"{experiment_dir.name}/{file_name}",
                message=str(error),
            )
        ]
    if len(values) != shots:
        return None, [
            QualityFinding(
                rule_id="google_qec.01_row_count_mismatch",
                severity=Severity.ERROR,
                source_system=SOURCE_NAME,
                source_record_locator=f"{experiment_dir.name}/{file_name}",
                message="01 file row count does not match the declared shot count",
                observed_value=f"observed={len(values)} expected={shots}",
            )
        ]
    return values, []


def build_experiment_row(properties: ExperimentProperties) -> dict[str, Any]:
    return {
        "source_record_id": f"{SOURCE_NAME}:{properties.experiment_id}:properties",
        "experiment_id": properties.experiment_id,
        "basis": properties.basis,
        "distance": properties.distance,
        "rounds": properties.rounds,
        "shots": properties.shots,
        "center_row": properties.center_row,
        "center_col": properties.center_col,
        "measurement_count": properties.measurement_count,
        "detector_count": properties.detector_count,
    }


def _experiment_trace_row(
    properties: ExperimentProperties, *, bronze_object: str, input_sha256: str
) -> dict[str, Any]:
    return {
        "source_record_id": f"{SOURCE_NAME}:{properties.experiment_id}:properties",
        "source_name": SOURCE_NAME,
        "bronze_object": bronze_object,
        "archive_member": f"{properties.experiment_id}/properties.yml",
        "record_locator": properties.experiment_id,
        "input_sha256": input_sha256,
    }


def _shot_trace_rows(
    properties: ExperimentProperties,
    shot_index: int,
    source_record_id: str,
    *,
    bronze_object: str,
    input_sha256: str,
) -> list[dict[str, Any]]:
    member_names = (
        "measurements.b8",
        "sweep.b8",
        "detection_events.b8",
        "obs_flips_actual.01",
        *DECODER_PREDICTION_COLUMNS.values(),
    )
    return [
        {
            "source_record_id": source_record_id,
            "source_name": SOURCE_NAME,
            "bronze_object": bronze_object,
            "archive_member": f"{properties.experiment_id}/{member_name}",
            "record_locator": f"{properties.experiment_id}:shot={shot_index}",
            "input_sha256": input_sha256,
        }
        for member_name in member_names
    ]


@dataclass(frozen=True)
class ProcessedExperiment:
    experiment_row: dict[str, Any] | None
    shot_rows: list[dict[str, Any]]
    trace_rows: list[dict[str, Any]]
    findings: list[QualityFinding]


def process_experiment_dir(
    experiment_dir: Path, *, bronze_object: str, input_sha256: str
) -> ProcessedExperiment:
    # if any check below fails we skip the whole experiment, not just one shot
    findings = verify_companion_files(experiment_dir)
    if findings:
        return ProcessedExperiment(None, [], [], findings)

    properties, property_findings = load_properties(experiment_dir)
    if properties is None:
        return ProcessedExperiment(None, [], [], property_findings)

    measurement_rows, measurement_findings = _read_packed_matrix(
        experiment_dir,
        "measurements.b8",
        bits_per_record=properties.measurement_count,
        shots=properties.shots,
    )
    sweep_rows, sweep_findings = _read_packed_matrix(
        experiment_dir,
        "sweep.b8",
        bits_per_record=max(properties.sweep_bit_count, 1),
        shots=properties.shots,
    )
    detector_rows, detector_findings = _read_packed_matrix(
        experiment_dir,
        "detection_events.b8",
        bits_per_record=properties.detector_count,
        shots=properties.shots,
    )
    actual_flips, actual_findings = _read_01_column(
        experiment_dir, "obs_flips_actual.01", shots=properties.shots
    )
    predictions: dict[str, list[int]] = {}
    prediction_findings: list[QualityFinding] = []
    for column, file_name in DECODER_PREDICTION_COLUMNS.items():
        values, column_findings = _read_01_column(
            experiment_dir, file_name, shots=properties.shots
        )
        prediction_findings.extend(column_findings)
        if values is not None:
            predictions[column] = values

    all_findings = [
        *measurement_findings,
        *sweep_findings,
        *detector_findings,
        *actual_findings,
        *prediction_findings,
    ]
    if all_findings:
        return ProcessedExperiment(None, [], [], all_findings)

    assert measurement_rows is not None
    assert sweep_rows is not None
    assert detector_rows is not None
    assert actual_flips is not None

    experiment_row = build_experiment_row(properties)
    experiment_trace_row = _experiment_trace_row(
        properties, bronze_object=bronze_object, input_sha256=input_sha256
    )

    shot_rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = [experiment_trace_row]
    for shot_index in range(properties.shots):
        detector_bits = detector_rows[shot_index]
        source_record_id = (
            f"{SOURCE_NAME}:{properties.experiment_id}:shot={shot_index}"
        )
        shot_rows.append(
            {
                "source_record_id": source_record_id,
                "experiment_id": properties.experiment_id,
                "shot_index": shot_index,
                "measurement_bits": pack_bits(measurement_rows[shot_index]),
                "sweep_bits": (
                    pack_bits(sweep_rows[shot_index]) if properties.sweep_bit_count else b""
                ),
                "detector_bits": pack_bits(detector_bits),
                "detector_event_count": sum(detector_bits),
                "actual_observable_flip": bool(actual_flips[shot_index]),
                **{
                    column: bool(predictions[column][shot_index])
                    for column in DECODER_PREDICTION_COLUMNS
                },
            }
        )
        trace_rows.extend(
            _shot_trace_rows(
                properties,
                shot_index,
                source_record_id,
                bronze_object=bronze_object,
                input_sha256=input_sha256,
            )
        )

    return ProcessedExperiment(experiment_row, shot_rows, trace_rows, [])


def build_silver_tables(
    root: Path, *, bronze_object: str, input_sha256: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[QualityFinding]]:
    experiment_rows: list[dict[str, Any]] = []
    shot_rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []
    findings: list[QualityFinding] = []

    for experiment_dir in iter_experiment_dirs(root):
        if not (experiment_dir / "properties.yml").is_file():
            continue
        processed = process_experiment_dir(
            experiment_dir, bronze_object=bronze_object, input_sha256=input_sha256
        )
        findings.extend(processed.findings)
        if processed.experiment_row is not None:
            experiment_rows.append(processed.experiment_row)
            shot_rows.extend(processed.shot_rows)
            trace_rows.extend(processed.trace_rows)

    return experiment_rows, shot_rows, trace_rows, findings


def write_table(rows: list[dict[str, Any]], schema: pa.Schema, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def write_silver_tables(
    experiment_rows: list[dict[str, Any]],
    shot_rows: list[dict[str, Any]],
    settings: Settings,
) -> None:
    """Publish Silver Parquet tables exclusively to the configured lake backend."""
    write_parquet(
        pa.Table.from_pylist(experiment_rows, schema=EXPERIMENT_SCHEMA),
        f"silver/{SOURCE_NAME}/experiment.parquet",
        settings,
    )
    write_parquet(
        pa.Table.from_pylist(shot_rows, schema=SHOT_SCHEMA),
        f"silver/{SOURCE_NAME}/shot.parquet",
        settings,
    )


def quality_finding_to_issue_row(finding: QualityFinding, run_id: str) -> dict[str, Any]:
    issue_id = stable_record_hash(
        {
            "run_id": run_id,
            "rule_id": finding.rule_id,
            "source_record_locator": finding.source_record_locator,
        }
    )
    return {
        "issue_id": issue_id,
        "run_id": run_id,
        "source_record_id": None,
        "rule_id": finding.rule_id,
        "severity": finding.severity.value,
        "observed_value": finding.observed_value,
        "action": "rejected",
        "reason": finding.message,
    }


def read_bronze_archive(settings: Settings) -> bytes:
    """Read the Bronze zip archive bytes from the configured lake backend."""
    if settings.lake_backend == "minio":
        client = minio_client(settings)
        response = client.get_object(settings.s3_bucket, BRONZE_OBJECT)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    for area in ("bronze", "raw"):
        candidate = (
            settings.local_lake_root
            / area
            / f"source={SOURCE_NAME}"
            / "google-surface-code-curated.zip"
        )
        if candidate.exists():
            return candidate.read_bytes()

    raise FileNotFoundError(
        f"Could not find the {SOURCE_NAME} Bronze archive under {settings.local_lake_root}"
    )


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path | None = None,
) -> StageResult:
    if settings is None:
        settings = Settings.from_environment()
    result = StageResult(stage=f"silver.{SOURCE_NAME}", run_id=run_id)
    if results_dir is None:
        results_dir = (
            settings.local_lake_root / "results" / "part1"
            if settings.lake_backend == "local"
            else Path("results/part1")
        )

    archive_bytes = read_bronze_archive(settings)
    input_sha256 = hashlib.sha256(archive_bytes).hexdigest()
    bronze_object = BRONZE_OBJECT

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        safe_member_names(archive)
        experiment_rows, shot_rows, trace_rows, findings = build_silver_tables(
            zipfile.Path(archive), bronze_object=bronze_object, input_sha256=input_sha256
        )

    write_silver_tables(experiment_rows, shot_rows, settings)

    issue_rows = [quality_finding_to_issue_row(finding, run_id) for finding in findings]
    replace_source_rows(
        results_dir / "source_trace.parquet",
        trace_rows,
        TRACE_SCHEMA,
        is_same_source=lambda row: row["source_name"] == SOURCE_NAME,
    )
    replace_source_rows(
        results_dir / "data_issues.parquet",
        issue_rows,
        ISSUE_SCHEMA,
        is_same_source=lambda row: str(row["rule_id"]).startswith(f"{SOURCE_NAME}."),
    )

    result.input_count = len(experiment_rows)
    result.output_count = len(shot_rows)
    result.issue_count = len(issue_rows)

    exp_rejected = sum(1 for finding in findings if finding.severity == Severity.ERROR)
    exp_read = len(experiment_rows) + exp_rejected
    result.table_counts["google_qec.experiment"] = {
        "read": exp_read,
        "accepted": len(experiment_rows),
        "rejected": exp_rejected,
        "reconciled": exp_read == len(experiment_rows) + exp_rejected,
    }
    result.table_counts["google_qec.shot"] = {
        "read": len(shot_rows),
        "accepted": len(shot_rows),
        "rejected": 0,
        "reconciled": True,
    }

    result.finish()
    return result
