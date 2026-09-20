"""Register and verify the original source files.

Student responsibilities:

- verify checksums from the release description;
- list source files and archive members safely;
- keep the supplied bytes unchanged;
- report missing or unexpected source objects;
- make a second run safe: no duplicate source records.
"""

from quantum_lake_student.models import StageResult


def run(run_id: str) -> StageResult:
    raise NotImplementedError("Implement source-file verification and registration")

