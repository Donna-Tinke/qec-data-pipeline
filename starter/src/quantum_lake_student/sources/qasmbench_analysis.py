"""QASMBench Part I analysis queries and export logic.

This module provides SQL queries and CSV export routines to answer Part I
Analysis Question 3 on repetition-code circuit mappings against PostgreSQL Gold.
"""

from __future__ import annotations

import csv
from pathlib import Path
import psycopg

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.models import StageResult


# =============================================================================
# Part I Analysis — Question 3: Repetition Code Mapping
# =============================================================================

QUESTION_3_QUERY = """
SELECT
    dq.data_qubit,
    s.ancilla_qubit,
    s.syndrome_bit,
    cc.condition_register,
    cc.condition_value,
    cc.gate AS recovery_gate
FROM gold.circuit c
JOIN gold.stabilizer_check s
    ON c.circuit_id = s.circuit_id
JOIN gold.stabilizer_data_qubit dq
    ON s.circuit_id = dq.circuit_id
   AND s.check_id = dq.check_id
JOIN gold.conditional_correction cc
    ON c.circuit_id = cc.circuit_id
   AND dq.data_qubit = cc.target_qubit
WHERE c.circuit_id = 'qec_sm_n5'
ORDER BY dq.data_qubit, s.check_id;
""".strip()

QUESTION_3_AGGREGATED_QUERY = """
SELECT
    dq.data_qubit,
    string_agg(DISTINCT s.ancilla_qubit, ', ' ORDER BY s.ancilla_qubit) AS parity_ancillas,
    string_agg(DISTINCT s.syndrome_bit, ', ' ORDER BY s.syndrome_bit) AS syndrome_bits,
    cc.condition_register,
    cc.condition_value,
    cc.gate AS recovery_gate
FROM gold.circuit c
JOIN gold.stabilizer_check s 
    ON c.circuit_id = s.circuit_id
JOIN gold.stabilizer_data_qubit dq 
    ON s.circuit_id = dq.circuit_id 
   AND s.check_id = dq.check_id
JOIN gold.conditional_correction cc 
    ON c.circuit_id = cc.circuit_id 
   AND dq.data_qubit = cc.target_qubit
WHERE c.circuit_id = 'qec_sm_n5'
GROUP BY 
    dq.data_qubit, 
    cc.condition_register, 
    cc.condition_value, 
    cc.gate
ORDER BY dq.data_qubit;
""".strip()


def run_question_3_analysis(
    conn: psycopg.Connection,
    output_dir: Path | None = None,
    write_aggregated: bool = True,
) -> Path:
    """Execute the Part I Question 3 SQL analysis and write results to CSV.

    Answers: "How does the repetition-code circuit map data qubits to parity-check
    ancillas, syndrome bits, and conditional corrections?"
    Joins 4 Gold tables (circuit, stabilizer_check, stabilizer_data_qubit,
    conditional_correction).

    Parameters
    ----------
    conn : psycopg.Connection
        Active PostgreSQL connection with gold schema populated.
    output_dir : Path | None
        Directory where CSV files will be stored. Defaults to 'results/part1/analysis'.
    write_aggregated : bool
        If True, also writes 'question_3_repetition_code_aggregated.csv' containing
        the grouped 1-row-per-qubit report.

    Returns
    -------
    Path
        Path to the primary relational output CSV file.
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


def run(
    run_id: str,
    settings: Settings | None = None,
    output_dir: Path | str | None = None,
    write_aggregated: bool = True,
) -> StageResult:
    """Execute Part I Question 3 analysis as a pipeline stage."""
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(stage="analysis.qasmbench", run_id=run_id)

    if output_dir is not None:
        out_path = Path(output_dir)
    else:
        out_path = Path("results/part1/analysis")

    with postgres_connection(settings) as conn:
        run_question_3_analysis(conn, output_dir=out_path, write_aggregated=write_aggregated)
        result.input_count = 1  # Target repetition code circuit analyzed ('qec_sm_n5')
        result.output_count = 2 if write_aggregated else 1  # Output CSV reports produced
        result.finish()

    return result

