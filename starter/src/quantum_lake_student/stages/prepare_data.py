"""Parse, check, and connect the supplied QEC data.

Apply documented checks, normalize nested/wide data, connect valid
relationships, and write prepared Parquet tables with chosen column names and
types. Write invalid records and the reason for exclusion to a machine-readable
data-issues output. The project README defines where generated files live.
"""

from quantum_lake_student.models import StageResult


def run(run_id: str) -> StageResult:
    raise NotImplementedError("Implement prepared and integrated QEC tables")
