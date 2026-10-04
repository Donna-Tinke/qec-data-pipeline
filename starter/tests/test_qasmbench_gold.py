"""Tests for QASMBench Gold relational schema, loading, and Question 3 SQL query."""

from __future__ import annotations

import csv
import os
from pathlib import Path
import shutil
import psycopg
import pytest

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.sources import qasmbench_analysis, qasmbench_gold, qasmbench_silver

# ---------------------------------------------------------------------------
# TOGGLE: Set USE_TEMP_STORAGE = False (or run with USE_TEMP_STORAGE=0)
# to write to live MinIO and the real results/part1/ directory.
# Defaults to True (isolated, test mode in tmp_path).
# ---------------------------------------------------------------------------
USE_TEMP_STORAGE = os.getenv("USE_TEMP_STORAGE", "1").lower() in ("1", "true", "yes")

REAL_ARCHIVE_CANDIDATES = [
    Path("/course-data/raw/source=qasmbench/qasmbench-qec.zip"),
    Path(__file__).resolve().parents[2]
    / "datasets/student-bundle/core/raw/source=qasmbench/qasmbench-qec.zip",
]


@pytest.fixture(autouse=True)
def setup_test_lake(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    if not USE_TEMP_STORAGE:
        # Live mode: do nothing, let Settings connect to real MinIO in Docker
        return

    # Local mode: redirect lake to tmp_path
    monkeypatch.setenv("LAKE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_LAKE_ROOT", str(tmp_path))

    source = next((p for p in REAL_ARCHIVE_CANDIDATES if p.exists()), None)
    if source is not None:
        target = tmp_path / "bronze/source=qasmbench/qasmbench-qec.zip"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, target)


@pytest.fixture
def target_results_dir(tmp_path: Path) -> Path:
    if not USE_TEMP_STORAGE:
        p = Path("results/part1")
        p.mkdir(parents=True, exist_ok=True)
        return p
    return tmp_path


@pytest.fixture
def db_conn(target_results_dir: Path):
    settings = Settings.from_environment()
    # Ensure Silver tables exist in the current lake environment
    qasmbench_silver.run("test-gold-fixture", settings=settings, results_dir=target_results_dir)
    try:
        with postgres_connection(settings) as conn:
            yield conn
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL server unreachable")


def test_qasmbench_gold_load_and_counts(db_conn: psycopg.Connection):
    counts = qasmbench_gold.load_qasmbench_gold(db_conn)

    assert counts["circuit"] == 6
    assert counts["circuit_register"] == 16
    assert counts["stabilizer_check"] == 4
    assert counts["stabilizer_data_qubit"] == 8
    assert counts["conditional_correction"] == 6

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM gold.circuit;")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT count(*) FROM gold.circuit_register;")
        assert cur.fetchone()[0] == 16

        cur.execute("SELECT count(*) FROM gold.stabilizer_check;")
        assert cur.fetchone()[0] == 4

        cur.execute("SELECT count(*) FROM gold.stabilizer_data_qubit;")
        assert cur.fetchone()[0] == 8

        cur.execute("SELECT count(*) FROM gold.conditional_correction;")
        assert cur.fetchone()[0] == 6


def test_qasmbench_gold_idempotence(db_conn: psycopg.Connection):
    counts1 = qasmbench_gold.load_qasmbench_gold(db_conn)
    counts2 = qasmbench_gold.load_qasmbench_gold(db_conn)

    assert counts1 == counts2

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM gold.circuit;")
        assert cur.fetchone()[0] == 6


def test_part1_question_3_analysis(db_conn: psycopg.Connection, target_results_dir: Path):
    """Test Part I Question 3 SQL queries and reproducible CSV export."""
    qasmbench_gold.load_qasmbench_gold(db_conn)

    # Execute analysis pipeline and generate both CSV outputs
    output_csv = qasmbench_analysis.run_question_3_analysis(db_conn, output_dir=target_results_dir)

    # 1. Verify primary relational CSV
    assert output_csv.exists()
    assert output_csv == target_results_dir / "question_3_repetition_code.csv"

    with open(output_csv, newline="", encoding="utf-8") as f:
        reader = list(csv.reader(f))

    assert reader[0] == [
        "data_qubit",
        "ancilla_qubit",
        "syndrome_bit",
        "condition_register",
        "condition_value",
        "recovery_gate",
    ]
    assert len(reader) == 5  # header + 4 rows
    assert reader[1] == ["q[0]", "a[0]", "syn[0]", "syn", "1", "x"]
    assert reader[2] == ["q[1]", "a[0]", "syn[0]", "syn", "3", "x"]
    assert reader[3] == ["q[1]", "a[1]", "syn[1]", "syn", "3", "x"]
    assert reader[4] == ["q[2]", "a[1]", "syn[1]", "syn", "2", "x"]

    # 2. Verify aggregated reporting CSV
    agg_csv = target_results_dir / "question_3_repetition_code_aggregated.csv"
    assert agg_csv.exists()

    with open(agg_csv, newline="", encoding="utf-8") as f:
        agg_reader = list(csv.reader(f))

    assert agg_reader[0] == [
        "data_qubit",
        "parity_ancillas",
        "syndrome_bits",
        "condition_register",
        "condition_value",
        "recovery_gate",
    ]
    assert len(agg_reader) == 4  # header + 3 rows
    assert agg_reader[1] == ["q[0]", "a[0]", "syn[0]", "syn", "1", "x"]
    assert agg_reader[2] == ["q[1]", "a[0], a[1]", "syn[0], syn[1]", "syn", "3", "x"]
    assert agg_reader[3] == ["q[2]", "a[1]", "syn[1]", "syn", "2", "x"]


def test_qasmbench_gold_stage_run(db_conn: psycopg.Connection):
    settings = Settings.from_environment()
    stage_res = qasmbench_gold.run("test-gold-stage-run", settings=settings)

    assert stage_res.stage == "gold.qasmbench"
    assert stage_res.run_id == "test-gold-stage-run"
    assert stage_res.input_count == 6
    assert stage_res.output_count == 40
    assert stage_res.finished_at is not None
    assert stage_res.finished_at >= stage_res.started_at


def test_qasmbench_analysis_stage_run(db_conn: psycopg.Connection, target_results_dir: Path):
    settings = Settings.from_environment()
    stage_res = qasmbench_analysis.run(
        "test-analysis-stage-run",
        settings=settings,
        output_dir=target_results_dir,
    )

    assert stage_res.stage == "analysis.qasmbench"
    assert stage_res.run_id == "test-analysis-stage-run"
    assert stage_res.input_count == 1
    assert stage_res.output_count == 2
    assert stage_res.finished_at is not None
    assert stage_res.finished_at >= stage_res.started_at
    assert (target_results_dir / "question_3_repetition_code.csv").exists()


