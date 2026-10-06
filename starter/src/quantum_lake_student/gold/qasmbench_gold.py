"""QASMBench Gold relational schema, PostgreSQL loader, and Part I SQL analysis.

This module defines the relational DDL for QASMBench in PostgreSQL (under schema 'gold'),
provides atomic, idempotent loading from Silver Parquet tables into Gold relations,
and executes the Part I Question 3 SQL analysis queries.
"""

from __future__ import annotations

import csv
import json
from contextlib import nullcontext
from pathlib import Path
import psycopg

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.models import StageResult
from quantum_lake_student.sources.qasmbench_silver import read_lake_table

SOURCE_NAME = "qasmbench"
SQL_DIR = Path(__file__).parent / "sql" / SOURCE_NAME


def _sql(name: str) -> str:
    """Read SQL query file from sql directory."""
    return (SQL_DIR / name).read_text(encoding="utf-8")


def init_schema(conn: psycopg.Connection) -> None:
    """Create the gold schema, tables, constraints, and indexes if they do not exist."""
    with conn.cursor() as cur:
        cur.execute(_sql("01_schema.sql"))


def load_qasmbench_gold(
    conn: psycopg.Connection,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Read QASMBench Silver Parquet tables from MinIO and load them atomically into PostgreSQL Gold.

    Uses an idempotent transaction so repeated executions update existing records
    without creating duplicates.
    """
    if settings is None:
        settings = Settings.from_environment()

    # 1. Ensure DDL exists
    init_schema(conn)

    # 2. Read Silver Parquet tables
    circuit_table = read_lake_table("silver/qasmbench/circuit.parquet", settings)
    stab_table = read_lake_table("silver/qasmbench/stabilizer_check.parquet", settings)
    cond_table = read_lake_table("silver/qasmbench/conditional_correction.parquet", settings)

    circuit_rows = circuit_table.to_pylist()
    stab_rows = stab_table.to_pylist()
    cond_rows = cond_table.to_pylist()

    counts = {
        "circuit": 0,
        "circuit_register": 0,
        "stabilizer_check": 0,
        "stabilizer_data_qubit": 0,
        "conditional_correction": 0,
    }

    # 3. Load within an atomic transaction
    with conn.transaction():
        with conn.cursor() as cur:
            # 3a. Populate gold.circuit & gold.circuit_register
            for row in circuit_rows:
                reg_decl = row["register_declarations"]
                if isinstance(reg_decl, str):
                    reg_decl_dict = json.loads(reg_decl)
                else:
                    reg_decl_dict = reg_decl

                cur.execute(
                    """
                    INSERT INTO gold.circuit (
                        circuit_id, benchmark_name, variant, qubit_count,
                        measurement_count, two_qubit_gate_count, source_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id) DO UPDATE SET
                        benchmark_name = EXCLUDED.benchmark_name,
                        variant = EXCLUDED.variant,
                        qubit_count = EXCLUDED.qubit_count,
                        measurement_count = EXCLUDED.measurement_count,
                        two_qubit_gate_count = EXCLUDED.two_qubit_gate_count,
                        source_record_id = EXCLUDED.source_record_id;
                    """,
                    (
                        row["circuit_id"],
                        row["benchmark_name"],
                        row["variant"],
                        row["qubit_count"],
                        row["measurement_count"],
                        row["two_qubit_gate_count"],
                        row["source_record_id"],
                    ),
                )
                counts["circuit"] += 1

                # Normalize registers into gold.circuit_register
                for reg_type, regs in [("qreg", reg_decl_dict.get("qregs", {})), ("creg", reg_decl_dict.get("cregs", {}))]:
                    for reg_name, size in regs.items():
                        cur.execute(
                            """
                            INSERT INTO gold.circuit_register (
                                circuit_id, register_name, register_type, size
                            ) VALUES (%s, %s, %s, %s)
                            ON CONFLICT (circuit_id, register_name) DO UPDATE SET
                                register_type = EXCLUDED.register_type,
                                size = EXCLUDED.size;
                            """,
                            (row["circuit_id"], reg_name, reg_type, int(size)),
                        )
                        counts["circuit_register"] += 1

            # 3b. Populate gold.stabilizer_check & gold.stabilizer_data_qubit
            for row in stab_rows:
                cur.execute(
                    """
                    INSERT INTO gold.stabilizer_check (
                        circuit_id, check_id, ancilla_qubit, syndrome_bit, source_record_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id, check_id) DO UPDATE SET
                        ancilla_qubit = EXCLUDED.ancilla_qubit,
                        syndrome_bit = EXCLUDED.syndrome_bit,
                        source_record_id = EXCLUDED.source_record_id;
                    """,
                    (
                        row["circuit_id"],
                        row["check_id"],
                        row["ancilla_qubit"],
                        row["syndrome_bit"],
                        row["source_record_id"],
                    ),
                )
                counts["stabilizer_check"] += 1

                # Normalize data qubits into gold.stabilizer_data_qubit
                d_qubits = row.get("data_qubits", [])
                for dq in d_qubits:
                    cur.execute(
                        """
                        INSERT INTO gold.stabilizer_data_qubit (
                            circuit_id, check_id, data_qubit
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (circuit_id, check_id, data_qubit) DO NOTHING;
                        """,
                        (row["circuit_id"], row["check_id"], dq),
                    )
                    counts["stabilizer_data_qubit"] += 1

            # 3c. Populate gold.conditional_correction using natural domain key
            for row in cond_rows:
                cur.execute(
                    """
                    INSERT INTO gold.conditional_correction (
                        circuit_id, condition_register, condition_value,
                        target_qubit, gate, source_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id, target_qubit, condition_register, condition_value) DO UPDATE SET
                        gate = EXCLUDED.gate,
                        source_record_id = EXCLUDED.source_record_id;
                    """,
                    (
                        row["circuit_id"],
                        row["condition_register"],
                        row["condition_value"],
                        row["target_qubit"],
                        row["gate"],
                        row["source_record_id"],
                    ),
                )
                counts["conditional_correction"] += 1

    return counts


def run(
    run_id: str,
    settings: Settings | None = None,
    conn: psycopg.Connection | None = None,
) -> StageResult:
    """Execute QASMBench Gold schema initialization and loading as a pipeline stage."""
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(stage="gold.qasmbench", run_id=run_id)

    conn_ctx = nullcontext(conn) if conn is not None else postgres_connection(settings)
    with conn_ctx as connection:
        counts = load_qasmbench_gold(connection, settings=settings)
        result.input_count = counts.get("circuit", 0)
        result.output_count = sum(counts.values())
        result.table_counts = {
            f"gold.{table}": count for table, count in counts.items()
        }
        result.finish()

    return result


# =============================================================================
# Part I Analysis — Question 3: Repetition Code Mapping
# =============================================================================

QUESTION_3_QUERY = _sql("analysis/question_3_repetition_code.sql").strip()
QUESTION_3_AGGREGATED_QUERY = _sql("analysis/question_3_repetition_code_aggregated.sql").strip()


def run_analyses(
    conn: psycopg.Connection,
    output_dir: Path | None = None,
    write_aggregated: bool = True,
) -> Path:
    """Execute the Part I Question 3 SQL analysis and write results to CSV.

    Answers: "How does the repetition-code circuit map data qubits to parity-check
    ancillas, syndrome bits, and conditional corrections?"
    Joins 4 Gold tables (circuit, stabilizer_check, stabilizer_data_qubit,
    conditional_correction).
    """
    if output_dir is None:
        output_dir = Path("results/part1/analysis")
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "question_3_repetition_code.csv"

    with conn.cursor() as cur:
        # 1. Execute standard 1NF relational query (4 rows)
        cur.execute(QUESTION_3_QUERY)
        headers = [col[0] for col in cur.description]
        rows = cur.fetchall()

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

        # 2. Optionally write aggregated report (3 rows)
        if write_aggregated:
            agg_path = output_dir / "question_3_repetition_code_aggregated.csv"
            cur.execute(QUESTION_3_AGGREGATED_QUERY)
            agg_headers = [col[0] for col in cur.description]
            agg_rows = cur.fetchall()
            with open(agg_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(agg_headers)
                writer.writerows(agg_rows)

    return csv_path
