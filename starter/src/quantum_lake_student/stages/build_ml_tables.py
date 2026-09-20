"""Create the required analyses and ML input tables from PostgreSQL Gold.

Write the Gold queries/views, joins, stable example IDs, and export logic.
A thin export step may execute SQL, use the course split helpers, validate the
fixed contracts, and write Parquet. It must not re-read Bronze or Silver.
"""

from quantum_lake_student.models import StageResult


def run(run_id: str) -> StageResult:
    raise NotImplementedError("Implement analysis outputs and ML input tables")
