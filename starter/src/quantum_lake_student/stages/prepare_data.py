"""Parse, check, and connect the supplied QEC data.

Apply documented checks, normalize nested/wide data, connect valid
relationships, and write prepared Parquet tables with chosen column names and
types. Write invalid records and the reason for exclusion to a machine-readable
data-issues output. The project README defines where generated files live.
"""

from __future__ import annotations

from quantum_lake_student.config import Settings
from quantum_lake_student.models import StageResult
from quantum_lake_student.sources import google_qec, qasmbench_silver, qec_syndromes


def run(run_id: str, settings: Settings | None = None) -> StageResult:
    """Execute the data preparation stage across all three QEC sources."""
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(stage="prepare_data", run_id=run_id)

    # 1. Google QEC Silver
    g_res = google_qec.run(run_id=run_id, settings=settings)

    # 2. QEC Syndromes Silver
    s_res = qec_syndromes.run(run_id=run_id, settings=settings)

    # 3. QASMBench Silver
    q_res = qasmbench_silver.run(run_id=run_id, settings=settings)

    result.input_count = g_res.input_count + s_res.input_count + q_res.input_count
    result.output_count = g_res.output_count + s_res.output_count + q_res.output_count
    result.issue_count = g_res.issue_count + s_res.issue_count + q_res.issue_count
    result.source_results = {
        "google_qec": g_res,
        "qec_syndromes": s_res,
        "qasmbench": q_res,
    }
    for sub in (g_res, s_res, q_res):
        result.table_counts.update(sub.table_counts)

    result.finish()
    return result
