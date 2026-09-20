"""google_qec source: companion-file checks, properties parsing, Silver rows.

Whole-archive hash verification lives in stages/register_sources.py, not here.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from quantum_lake_student.formats import b8_record_bytes, iter_b8_records, parse_01_records
from quantum_lake_student.models import QualityFinding, Severity

SOURCE_NAME = "google_qec"

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
    # any failed check below rejects the whole experiment, not just one shot
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
                "measurement_bits": bytes(measurement_rows[shot_index]),
                "sweep_bits": bytes(sweep_rows[shot_index]),
                "detector_bits": bytes(detector_bits),
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
    archive_root: Path, *, bronze_object: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[QualityFinding]]:
    input_sha256 = sha256_file(archive_root) if archive_root.is_file() else ""
    experiment_rows: list[dict[str, Any]] = []
    shot_rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []
    findings: list[QualityFinding] = []

    root = archive_root if archive_root.is_dir() else archive_root.parent
    for experiment_dir in iter_experiment_dirs(root):
        if not (experiment_dir / "properties.yml").exists():
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
