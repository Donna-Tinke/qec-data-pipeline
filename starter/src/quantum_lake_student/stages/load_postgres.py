"""Relational integration stage.

Create the student-designed PostgreSQL tables and load them as one all-or-
nothing update. Primary/foreign keys, value checks, and indexes are part of the
deliverable. Repeated loads must not create duplicate records.
"""

from quantum_lake_student.models import StageResult


def run(run_id: str) -> StageResult:
    raise NotImplementedError("Implement all-or-nothing PostgreSQL loading")
