import io
import shutil
import zipfile
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantum_lake_student.config import Settings
from quantum_lake_student.ml import syndrome_data_split, syndrome_model_input
from quantum_lake_student.sources import qec_syndromes
from quantum_lake_student.sources.qec_syndromes import (
    ISSUE_SCHEMA,
    RULE_COLUMN_COUNT,
    RULE_CSV_COUNT,
    RULE_DISTANCE,
    RULE_DUPLICATE_KEY,
    RULE_FILENAME,
    RULE_HEADER,
    RULE_HEADER_DOCUMENTED,
    RULE_LABEL_DOMAIN,
    RULE_QUANTITY,
    RULE_SYNDROME_DOMAIN,
    RULE_SYNDROME_PARSE,
    RULE_SYNDROME_SHAPE,
    RULE_WEIGHTED_TOTAL,
    SYNDROME_OBSERVATION_SCHEMA,
    TRACE_SCHEMA,
    RowRejected,
    build_silver_rows,
    parse_filename,
    parse_syndrome,
    process_csv,
)

MEMBER = "d-3_pfr-0.001000_nb-10M.csv"
ZERO = '"((0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))"'
ONE_BIT = '"((1, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 1))"'


def make_csv(*rows: str, header: str = "labels,syndromes,quantity") -> bytes:
    return ("\n".join([header, *rows]) + "\n").encode()


def rule_ids(result) -> list[str]:
    return [issue.rule_id for issue in result.issues]


def make_zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


# --- filename --------------------------------------------------------------


def test_filename_gives_fault_rate_and_sample_count() -> None:
    meta = parse_filename("d-3_pfr-0.000500_nb-10M.csv")
    assert meta is not None
    assert meta.experiment_id == "d-3_pfr-0.000500_nb-10M"
    assert meta.distance == 3
    assert meta.physical_fault_rate == 0.0005
    assert meta.nominal_sample_count == 10_000_000
    # must land in the course validation split
    assert syndrome_data_split(meta.physical_fault_rate) == "validation"


def test_bad_filename_rejects_the_whole_file() -> None:
    result = process_csv("syndromes.csv", make_csv(f"0,{ZERO},5"), input_sha256="x")
    assert rule_ids(result) == [RULE_FILENAME]
    assert result.observations == []
    assert result.checks["rows_rejected"] == 1


def test_other_distance_rejects_the_whole_file() -> None:
    result = process_csv("d-5_pfr-0.001000_nb-10M.csv", make_csv(f"0,{ZERO},5"), input_sha256="x")
    assert rule_ids(result) == [RULE_DISTANCE]
    assert result.observations == []


# --- header ----------------------------------------------------------------


def test_actual_labels_header_is_accepted_with_documented_warning() -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ZERO},10000000"), input_sha256="x")
    assert rule_ids(result) == [RULE_HEADER_DOCUMENTED]
    assert result.issues[0].action == "kept"
    assert len(result.observations) == 1


def test_documented_label_header_is_also_accepted() -> None:
    result = process_csv(
        MEMBER, make_csv(f"0,{ZERO},10000000", header="label,syndromes,quantity"), input_sha256="x"
    )
    assert rule_ids(result) == []
    assert len(result.observations) == 1


def test_unknown_header_rejects_the_whole_file() -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ZERO},5", header="y,x,n"), input_sha256="x")
    assert rule_ids(result) == [RULE_HEADER]
    assert result.observations == []


# --- syndrome shape and domain ---------------------------------------------


def test_syndrome_is_flattened_round_first_then_check() -> None:
    bits = parse_syndrome("((1, 0, 0, 0), (0, 1, 0, 0), (0, 0, 1, 0), (0, 0, 0, 1))")
    assert bits == (1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1)
    assert syndrome_model_input(bits) == bits


@pytest.mark.parametrize(
    ("raw", "rule"),
    [
        ("((0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))", RULE_SYNDROME_SHAPE),
        ("((0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))", RULE_SYNDROME_SHAPE),
        ("((0, 0, 0, 2), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))", RULE_SYNDROME_DOMAIN),
        ("((0, 0, 0, True), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))", RULE_SYNDROME_DOMAIN),
        ("((0, 0, 0, 0.0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))", RULE_SYNDROME_DOMAIN),
        ("((0, 0, 0, 0), (0, 0", RULE_SYNDROME_PARSE),
        ("__import__('os')", RULE_SYNDROME_PARSE),
    ],
)
def test_bad_syndromes_are_rejected(raw: str, rule: str) -> None:
    with pytest.raises(RowRejected) as err:
        parse_syndrome(raw)
    assert err.value.rule_id == rule


# --- row-level rejects go to data_issues with the original value -----------


@pytest.mark.parametrize(
    ("row", "rule"),
    [
        (f"2,{ZERO},5", RULE_LABEL_DOMAIN),
        (f"0,{ZERO},0", RULE_QUANTITY),
        (f"0,{ZERO},-3", RULE_QUANTITY),
        (f"0,{ZERO},1.5", RULE_QUANTITY),
        (f"0,{ZERO}", RULE_COLUMN_COUNT),
        ('0,"((0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 9))",5', RULE_SYNDROME_DOMAIN),
    ],
)
def test_invalid_row_is_excluded_and_reported(row: str, rule: str) -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ONE_BIT},10000000", row), input_sha256="x")
    errors = [issue for issue in result.issues if issue.rule_id == rule]
    assert len(errors) == 1
    issue = errors[0]
    assert issue.action == "rejected"
    assert issue.observed_value == row
    assert issue.source_record_id == f"qec_syndromes:{MEMBER}:line=3"
    # the rejected row is still traceable
    assert issue.source_record_id in {t["source_record_id"] for t in result.trace_rows}
    assert len(result.observations) == 1
    assert result.checks["rows_read"] == 2
    assert result.checks["rows_accepted"] + result.checks["rows_rejected"] == 2


# --- weights and labels ----------------------------------------------------


def test_quantity_is_a_weight_not_expanded() -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ZERO},9999990", f"1,{ONE_BIT},10"), input_sha256="x")
    assert len(result.observations) == 2
    assert [row["quantity"] for row in result.observations] == [9999990, 10]
    assert result.checks["weighted_total_matches_filename"]
    assert RULE_WEIGHTED_TOTAL not in rule_ids(result)


def test_weighted_total_mismatch_is_a_warning_and_rows_stay() -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ZERO},5"), input_sha256="x")
    warning = [issue for issue in result.issues if issue.rule_id == RULE_WEIGHTED_TOTAL]
    assert len(warning) == 1 and warning[0].action == "kept"
    assert warning[0].observed_value == "5"
    assert len(result.observations) == 1


def test_same_syndrome_with_both_labels_is_valid() -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ONE_BIT},9999999", f"1,{ONE_BIT},1"), input_sha256="x")
    assert len(result.observations) == 2
    assert {row["logical_error_label"] for row in result.observations} == {False, True}
    assert RULE_DUPLICATE_KEY not in rule_ids(result)
    assert result.checks["syndromes_with_both_labels"] == 1


def test_repeated_syndrome_and_label_is_kept_with_warning() -> None:
    result = process_csv(MEMBER, make_csv(f"0,{ONE_BIT},9999999", f"0,{ONE_BIT},1"), input_sha256="x")
    assert len(result.observations) == 2
    assert rule_ids(result).count(RULE_DUPLICATE_KEY) == 1


# --- archive level ---------------------------------------------------------


def test_unsafe_member_stops_the_run() -> None:
    archive = make_zip({"../evil.csv": make_csv(f"0,{ZERO},5")})
    with pytest.raises(ValueError, match="unsafe"):
        build_silver_rows(archive, input_sha256="x")


def test_readme_is_skipped_and_csv_count_checked() -> None:
    archive = make_zip({"README.txt": b"docs", MEMBER: make_csv(f"0,{ZERO},10000000")})
    built = build_silver_rows(archive, input_sha256="x")
    assert len(built.observations) == 1
    assert RULE_CSV_COUNT in {issue.rule_id for issue in built.issues}
    assert built.checks["rows_reconcile"]


def test_issue_id_is_stable_across_runs() -> None:
    result = process_csv(MEMBER, make_csv(f"9,{ZERO},5"), input_sha256="x")
    first = [issue.to_row("run-1") for issue in result.issues]
    second = [issue.to_row("run-2") for issue in result.issues]
    assert [row["issue_id"] for row in first] == [row["issue_id"] for row in second]


# --- full run on the real Bronze zip ---------------------------------------

REAL_ARCHIVE_CANDIDATES = [
    Path("/course-data/raw/source=qec_syndromes/syndromes_dataset.zip"),
    Path(__file__).resolve().parents[2]
    / "datasets/student-bundle/core/raw/source=qec_syndromes/syndromes_dataset.zip",
]


@pytest.fixture
def local_lake(tmp_path: Path) -> tuple[Settings, Path]:
    source = next((path for path in REAL_ARCHIVE_CANDIDATES if path.exists()), None)
    if source is None:
        pytest.skip("real syndromes_dataset.zip not available")
    target = tmp_path / "lake/bronze/source=qec_syndromes/syndromes_dataset.zip"
    target.parent.mkdir(parents=True)
    shutil.copy(source, target)
    settings = replace(
        Settings.from_environment(), lake_backend="local", local_lake_root=tmp_path / "lake"
    )
    return settings, tmp_path / "results/part1"


def test_full_run_on_real_data(local_lake: tuple[Settings, Path]) -> None:
    settings, results_dir = local_lake
    stage = qec_syndromes.run("test-run", settings=settings, results_dir=results_dir)

    silver = pq.read_table(settings.local_lake_root / "silver/qec_syndromes/syndrome_observation.parquet")
    trace = pq.read_table(results_dir / "source_trace.parquet")
    issues = pq.read_table(results_dir / "data_issues.parquet")

    assert silver.schema.equals(SYNDROME_OBSERVATION_SCHEMA)
    assert trace.schema.equals(TRACE_SCHEMA)
    assert issues.schema.equals(ISSUE_SCHEMA)

    # 75,598 aggregate rows representing 70M shots (ASSIGNMENT_SPEC.md)
    assert stage.input_count == 75_598
    assert silver.num_rows == 75_598
    assert sum(silver["quantity"].to_pylist()) == 70_000_000
    assert len(set(silver["experiment_id"].to_pylist())) == 7

    rows = silver.to_pylist()
    assert all(len(row["syndrome_bits"]) == 16 for row in rows)
    assert all(set(row["syndrome_bits"]) <= {0, 1} for row in rows)
    assert all(row["round_count"] == 4 and row["check_count"] == 4 for row in rows)
    assert all(row["quantity"] > 0 for row in rows)
    assert len({row["source_record_id"] for row in rows}) == len(rows)

    # only the expected header warning, one per CSV; nothing rejected
    issue_rows = issues.to_pylist()
    assert {row["rule_id"] for row in issue_rows} == {RULE_HEADER_DOCUMENTED}
    assert len(issue_rows) == 7

    # every Silver row resolves to a Bronze trace
    trace_ids = set(trace["source_record_id"].to_pylist())
    assert set(silver["source_record_id"].to_pylist()) <= trace_ids

    # reconciliation: rows read = rows accepted + rows rejected
    rejected = sum(1 for row in issue_rows if row["action"] == "rejected")
    assert stage.input_count == stage.output_count + rejected

    split_weights: dict[str, int] = {}
    for row in rows:
        split = syndrome_data_split(row["physical_fault_rate"])
        split_weights[split] = split_weights.get(split, 0) + row["quantity"]
    assert split_weights == {"train": 50_000_000, "validation": 10_000_000, "test": 10_000_000}


def test_second_run_gives_identical_output(local_lake: tuple[Settings, Path]) -> None:
    settings, results_dir = local_lake
    silver_path = settings.local_lake_root / "silver/qec_syndromes/syndrome_observation.parquet"

    qec_syndromes.run("run-1", settings=settings, results_dir=results_dir)
    silver_1 = pq.read_table(silver_path)
    trace_1 = pq.read_table(results_dir / "source_trace.parquet")
    issue_ids_1 = pq.read_table(results_dir / "data_issues.parquet")["issue_id"].to_pylist()

    qec_syndromes.run("run-2", settings=settings, results_dir=results_dir)
    silver_2 = pq.read_table(silver_path)
    trace_2 = pq.read_table(results_dir / "source_trace.parquet")
    issue_ids_2 = pq.read_table(results_dir / "data_issues.parquet")["issue_id"].to_pylist()

    assert silver_1.equals(silver_2)
    assert trace_1.equals(trace_2)
    assert issue_ids_1 == issue_ids_2


def test_run_keeps_other_sources_rows(local_lake: tuple[Settings, Path]) -> None:
    settings, results_dir = local_lake
    results_dir.mkdir(parents=True)
    other = {
        "source_record_id": "qasmbench:small/x.qasm",
        "source_name": "qasmbench",
        "bronze_object": "b",
        "archive_member": "m",
        "record_locator": None,
        "input_sha256": "h",
    }
    pq.write_table(pa.Table.from_pylist([other], schema=TRACE_SCHEMA), results_dir / "source_trace.parquet")
    qec_syndromes.run("run", settings=settings, results_dir=results_dir)
    trace = pq.read_table(results_dir / "source_trace.parquet").to_pylist()
    assert other in trace
