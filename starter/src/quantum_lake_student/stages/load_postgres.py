"""Relational integration stage.

Create the student-designed PostgreSQL tables and load them as one all-or-
nothing update. Primary/foreign keys, value checks, and indexes are part of the
deliverable. Repeated loads must not create duplicate records.
"""

from __future__ import annotations

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.gold import (
    google_qec as gold_google_qec,
    qasmbench_gold,
    qec_syndromes as gold_qec_syndromes,
)
from quantum_lake_student.models import StageResult


def run(run_id: str, settings: Settings | None = None) -> StageResult:
    """Execute relational integration stage for available sources."""
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(stage="load_postgres", run_id=run_id)

    with postgres_connection(settings) as conn:
        with conn.transaction():
            # 1. Google QEC Gold tables
            g_res = gold_google_qec.run(run_id=run_id, settings=settings, connection=conn)

            # 2. QASMBench Gold tables
            q_res = qasmbench_gold.run(run_id=run_id, settings=settings, conn=conn)

            # 3. QEC Syndromes Gold tables
            s_res = gold_qec_syndromes.run(run_id=run_id, settings=settings, conn=conn)

    result.input_count = g_res.input_count + q_res.input_count + s_res.input_count
    result.output_count = g_res.output_count + q_res.output_count + s_res.output_count
    result.issue_count = g_res.issue_count + q_res.issue_count + s_res.issue_count
    result.source_results = {
        "gold_google_qec": g_res,
        "gold_qasmbench": q_res,
        "gold_qec_syndromes": s_res,
    }
    result.table_counts.update(g_res.table_counts)
    result.table_counts.update(q_res.table_counts)
    result.table_counts.update(s_res.table_counts)

    result.finish()
    return result
