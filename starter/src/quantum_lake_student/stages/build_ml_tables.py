"""Create the required analyses and ML input tables from PostgreSQL Gold.

Write the Gold queries/views, joins, stable example IDs, and export logic.
A thin export step may execute SQL, use the course split helpers, validate the
fixed contracts, and write Parquet. It must not re-read Bronze or Silver.
"""

from __future__ import annotations

from pathlib import Path

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.models import StageResult


def export_google_ml_table(settings: Settings) -> int:
    """Export gold.v_ml_google_decoder_example to ml/ml_google_decoder_example.parquet."""
    from quantum_lake_student.gold import google_qec as gold_google_qec
    from quantum_lake_student.lake import write_parquet

    with postgres_connection(settings) as conn:
        conn.execute("SET search_path TO gold, public")
        table = gold_google_qec.export_ml_examples(conn)

    write_parquet(table, gold_google_qec.ML_OBJECT, settings)
    return table.num_rows


def export_syndrome_ml_table(settings: Settings) -> int:
    """Export gold.v_ml_syndrome_decoder_example to ml/ml_syndrome_decoder_example.parquet."""
    from quantum_lake_student.gold import qec_syndromes as gold_qec_syndromes
    from quantum_lake_student.lake import write_parquet

    with postgres_connection(settings) as conn:
        conn.execute("SET search_path TO gold, public")
        table = gold_qec_syndromes.export_ml_examples(conn)

    write_parquet(table, gold_qec_syndromes.ML_OBJECT, settings)
    return table.num_rows


def run_gold_analyses(settings: Settings, analysis_dir: Path) -> None:
    """Execute all three Part I SQL analyses (Q1, Q2, Q3) against PostgreSQL Gold."""
    from quantum_lake_student.gold import (
        google_qec as gold_google_qec,
        qasmbench_gold,
        qec_syndromes as gold_qec_syndromes,
    )

    analysis_dir.mkdir(parents=True, exist_ok=True)
    with postgres_connection(settings) as conn:
        conn.execute("SET search_path TO gold, public")
        # Question 1: QEC Syndromes analysis
        gold_qec_syndromes.run_analyses(conn, analysis_dir)
        # Question 2: Google QEC analysis & detector storage comparison
        gold_google_qec.run_analyses(conn, analysis_dir)
        # Question 3: QASMBench repetition-code analysis
        qasmbench_gold.run_analyses(conn, output_dir=analysis_dir)


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path | str | None = None,
) -> StageResult:
    """Create the required analyses and ML input tables from PostgreSQL Gold."""
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(stage="build_ml_tables", run_id=run_id)
    if results_dir is not None:
        r_dir = Path(results_dir)
    else:
        r_dir = (
            settings.local_lake_root / "results" / "part1"
            if settings.lake_backend == "local"
            else Path("results/part1")
        )

    # 1. Export Google ML table
    google_count = export_google_ml_table(settings)

    # 2. Export Syndrome ML table
    syndrome_count = export_syndrome_ml_table(settings)

    # 3. Run all three Part I SQL analyses against committed PostgreSQL Gold
    run_gold_analyses(settings, r_dir / "analysis")

    result.input_count = google_count + syndrome_count
    result.output_count = google_count + syndrome_count

    result.finish()
    return result
