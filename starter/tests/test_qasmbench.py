from pathlib import Path
import pytest
import pyarrow.parquet as pq

from quantum_lake_student.config import Settings
from quantum_lake_student.models import Severity
from quantum_lake_student.sources import qasmbench
from quantum_lake_student.sources.qasmbench import (
    CIRCUIT_SCHEMA,
    CONDITIONAL_SCHEMA,
    DATA_ISSUES_SCHEMA,
    RULE_PARSE_ERROR,
    SOURCE_TRACE_SCHEMA,
    STABILIZER_SCHEMA,
    get_file_buffer,
    parse_qasm,
    read_lake_table
)


def test_parse_valid_qasm() -> None:
    settings = Settings.from_environment()
    bronze_object = "bronze/source=qasmbench/qasmbench-qec.zip"
    archive_member = "small/qec_sm_n5/qec_sm_n5.qasm"
    content = get_file_buffer(settings, bronze_object, member_name=archive_member).getvalue().decode("utf-8")

    source_records, circuit, stabs, conds, findings = parse_qasm(
        content=content,
        source_name="qasmbench",
        bronze_object=bronze_object,
        archive_member=archive_member,
        archive_sha256="fake_sha256",
    )
    assert not any(f.severity == Severity.ERROR for f in findings)
    assert circuit is not None
    assert circuit["circuit_id"] == "qec_sm_n5"
    assert circuit["benchmark_name"] == "qec_sm"
    assert circuit["variant"] == "source"
    assert circuit["qubit_count"] == 5
    assert circuit["measurement_count"] == 5
    assert circuit["two_qubit_gate_count"] == 4

    assert len(stabs) == 2
    assert stabs[0]["ancilla_qubit"] == "a[0]"
    assert stabs[0]["data_qubits"] == ["q[0]", "q[1]"]
    assert stabs[0]["syndrome_bit"] == "syn[0]"
    assert stabs[1]["ancilla_qubit"] == "a[1]"
    assert stabs[1]["data_qubits"] == ["q[1]", "q[2]"]
    assert stabs[1]["syndrome_bit"] == "syn[1]"

    assert len(conds) == 3
    assert conds[0]["condition_register"] == "syn"
    assert conds[0]["condition_value"] == 1
    assert conds[0]["gate"] == "x"
    assert conds[0]["target_qubit"] == "q[0]"


def test_parse_error_records_issue() -> None:
    source_records, circuit, stabs, conds, findings = parse_qasm(
        content="// file without any register declarations\nx q[0];\n",
        source_name="qasmbench",
        bronze_object="test",
        archive_member="small/bad/bad.qasm",
        archive_sha256="fake",
    )
    assert circuit is None
    assert any(f.rule_id == RULE_PARSE_ERROR for f in findings)


def test_qasmbench_stage_execution() -> None:
    settings = Settings.from_environment()
    res = qasmbench.run("test-integration-run", settings=settings)

    assert res.stage == "silver.qasmbench"
    assert res.run_id == "test-integration-run"
    assert res.input_count == 6
    assert res.output_count == 16
    assert res.issue_count == 0
    assert res.finished_at is not None
    assert res.finished_at >= res.started_at

    # Check that silver parquet tables were written to the lake
    circuit_table = read_lake_table("silver/qasmbench/circuit.parquet", settings)
    stab_table = read_lake_table("silver/qasmbench/stabilizer_check.parquet", settings)
    cond_table = read_lake_table("silver/qasmbench/conditional_correction.parquet", settings)

    # Check that result tables were written to results/part1/
    trace_p = Path("results/part1/source_trace.parquet")
    issues_p = Path("results/part1/data_issues.parquet")

    assert trace_p.exists()
    assert issues_p.exists()

    trace_table = pq.read_table(trace_p)
    issues_table = pq.read_table(issues_p)

    # Verify PyArrow schemas match assignment specification
    assert circuit_table.schema.equals(CIRCUIT_SCHEMA)
    assert stab_table.schema.equals(STABILIZER_SCHEMA)
    assert cond_table.schema.equals(CONDITIONAL_SCHEMA)
    assert trace_table.schema.equals(SOURCE_TRACE_SCHEMA)
    assert issues_table.schema.equals(DATA_ISSUES_SCHEMA)

    # Verify counts
    assert circuit_table.num_rows == 6
    assert stab_table.num_rows == 4
    assert cond_table.num_rows == 6

    # Verify counts for qasmbench in shared result tables
    qasm_trace_rows = [r for r in trace_table.to_pylist() if r["source_name"] == "qasmbench"]
    assert len(qasm_trace_rows) == 15

    qasm_issue_rows = [r for r in issues_table.to_pylist() if str(r["rule_id"]).startswith("qasmbench")]
    assert len(qasm_issue_rows) == 0

    # trace coverage (every Silver row traces back to source_trace)
    qasm_trace_ids = {r["source_record_id"] for r in qasm_trace_rows}
    assert set(circuit_table["source_record_id"].to_pylist()).issubset(qasm_trace_ids)
    assert set(stab_table["source_record_id"].to_pylist()).issubset(qasm_trace_ids)
    assert set(cond_table["source_record_id"].to_pylist()).issubset(qasm_trace_ids)

    # row reconciliation (rows read == rows accepted + rows rejected)
    assert res.input_count == circuit_table.num_rows + len(qasm_issue_rows)


def test_safe_repeated_runs_idempotence() -> None:
    settings = Settings.from_environment()
    run1 = qasmbench.run("run-1", settings=settings)
    circuit_t1 = read_lake_table("silver/qasmbench/circuit.parquet", settings)
    stab_t1 = read_lake_table("silver/qasmbench/stabilizer_check.parquet", settings)
    cond_t1 = read_lake_table("silver/qasmbench/conditional_correction.parquet", settings)
    trace_t1 = pq.read_table("results/part1/source_trace.parquet")

    run2 = qasmbench.run("run-2", settings=settings)
    circuit_t2 = read_lake_table("silver/qasmbench/circuit.parquet", settings)
    stab_t2 = read_lake_table("silver/qasmbench/stabilizer_check.parquet", settings)
    cond_t2 = read_lake_table("silver/qasmbench/conditional_correction.parquet", settings)
    trace_t2 = pq.read_table("results/part1/source_trace.parquet")

    # Verify all tables are identical across repeated runs (no duplicates, stable IDs)
    assert circuit_t1.equals(circuit_t2)
    assert stab_t1.equals(stab_t2)
    assert cond_t1.equals(cond_t2)
    assert trace_t1.equals(trace_t2)
    assert run1.output_count == run2.output_count




