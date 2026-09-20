from pathlib import Path

import pytest

from quantum_lake_student.sources import google_qec

def _properties_yml(center_row: int, center_col: int) -> str:
    return f"""\
type: surface_code_memory_experiment
basis: X
rounds: 2
distance: 3
shots: 3
center_data_qubit_row: {center_row}
center_data_qubit_col: {center_col}
circuit_measurements: 5
circuit_sweep_bits: 2
circuit_detectors: 4
circuit_observables: 1
"""


# 3 shots, 1 byte each. Top 3 bits of the measurement byte and top 4 bits of
# the detector byte are padding and must be zero.
MEASUREMENT_BYTES = bytes([0b00010101, 0b00010101, 0b00010101])
SWEEP_BYTES = bytes([0b00000001, 0b00000001, 0b00000001])
DETECTOR_BYTES = bytes([0b00001010, 0b00001010, 0b00001010])  # 2 set bits per shot


def _write_experiment(
    root: Path,
    name: str = "surface_code_bX_d3_r2_center_1_2",
    *,
    center_row: int = 1,
    center_col: int = 2,
) -> Path:
    experiment_dir = root / name
    experiment_dir.mkdir()
    (experiment_dir / "properties.yml").write_text(_properties_yml(center_row, center_col))
    (experiment_dir / "measurements.b8").write_bytes(MEASUREMENT_BYTES)
    (experiment_dir / "sweep.b8").write_bytes(SWEEP_BYTES)
    (experiment_dir / "detection_events.b8").write_bytes(DETECTOR_BYTES)
    (experiment_dir / "obs_flips_actual.01").write_bytes(b"0\n1\n0\n")
    for file_name in google_qec.DECODER_PREDICTION_COLUMNS.values():
        (experiment_dir / file_name).write_bytes(b"0\n1\n1\n")
    return experiment_dir


def test_verify_companion_files_reports_each_missing_file(tmp_path: Path) -> None:
    experiment_dir = _write_experiment(tmp_path)
    (experiment_dir / "sweep.b8").unlink()

    findings = google_qec.verify_companion_files(experiment_dir)

    assert len(findings) == 1
    assert findings[0].rule_id == "google_qec.missing_companion_file"
    assert "sweep.b8" in findings[0].source_record_locator


def test_load_properties_rejects_directory_mismatch(tmp_path: Path) -> None:
    experiment_dir = _write_experiment(tmp_path, name="surface_code_bX_d5_r2_center_1_2")

    properties, findings = google_qec.load_properties(experiment_dir)

    assert properties is None
    assert findings[0].rule_id == "google_qec.directory_properties_mismatch"


def test_load_properties_parses_matching_directory(tmp_path: Path) -> None:
    experiment_dir = _write_experiment(tmp_path)

    properties, findings = google_qec.load_properties(experiment_dir)

    assert findings == []
    assert properties is not None
    assert properties.distance == 3
    assert properties.shots == 3
    assert properties.measurement_count == 5
    assert properties.detector_count == 4


def test_process_experiment_dir_builds_silver_rows(tmp_path: Path) -> None:
    experiment_dir = _write_experiment(tmp_path)

    processed = google_qec.process_experiment_dir(
        experiment_dir, bronze_object="raw/source=google_qec/google-surface-code-curated.zip",
        input_sha256="deadbeef",
    )

    assert processed.findings == []
    assert processed.experiment_row is not None
    assert processed.experiment_row["shots"] == 3
    assert processed.experiment_row["detector_count"] == 4
    assert len(processed.shot_rows) == 3

    first_shot = processed.shot_rows[0]
    assert first_shot["detector_event_count"] == 2
    assert first_shot["actual_observable_flip"] is False
    assert first_shot["belief_matching_prediction"] is False

    # one experiment trace row plus 8 companion-file trace rows per shot
    assert len(processed.trace_rows) == 1 + 3 * 8


def test_process_experiment_dir_rejects_bad_b8_padding(tmp_path: Path) -> None:
    experiment_dir = _write_experiment(tmp_path)
    # Set a padding bit (bit 7) in the measurement byte, which must be zero.
    corrupted = bytes([0b10010101, 0b00010101, 0b00010101])
    (experiment_dir / "measurements.b8").write_bytes(corrupted)

    processed = google_qec.process_experiment_dir(
        experiment_dir, bronze_object="bronze_object", input_sha256="hash"
    )

    assert processed.experiment_row is None
    assert any(f.rule_id == "google_qec.b8_padding_not_zero" for f in processed.findings)


def test_process_experiment_dir_rejects_length_mismatch(tmp_path: Path) -> None:
    experiment_dir = _write_experiment(tmp_path)
    (experiment_dir / "detection_events.b8").write_bytes(DETECTOR_BYTES[:2])  # too short

    processed = google_qec.process_experiment_dir(
        experiment_dir, bronze_object="bronze_object", input_sha256="hash"
    )

    assert processed.experiment_row is None
    assert any(f.rule_id == "google_qec.b8_length_mismatch" for f in processed.findings)


def test_build_silver_tables_over_directory_root(tmp_path: Path) -> None:
    _write_experiment(tmp_path, name="surface_code_bX_d3_r2_center_1_2", center_row=1, center_col=2)
    _write_experiment(tmp_path, name="surface_code_bX_d3_r2_center_9_9", center_row=9, center_col=9)

    experiment_rows, shot_rows, trace_rows, findings = google_qec.build_silver_tables(
        tmp_path, bronze_object="bronze_object"
    )

    assert findings == []
    assert len(experiment_rows) == 2
    assert len(shot_rows) == 6
    assert {row["experiment_id"] for row in experiment_rows} == {
        "surface_code_bX_d3_r2_center_1_2",
        "surface_code_bX_d3_r2_center_9_9",
    }
