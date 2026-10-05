"""Relational integration stage.

Create the student-designed PostgreSQL tables and load them as one all-or-
nothing update. Primary/foreign keys, value checks, and indexes are part of the
deliverable. Repeated loads must not create duplicate records.

Team convention: every source loader rebuilds only its own objects inside
schema `gold`, within `connection.transaction()`. run() wraps all registered
loaders in ONE outer transaction; psycopg turns each loader's own block into a
savepoint, so a failure in any source rolls back every source and the previous
complete Gold stays. Loaders drop and rebuild their tables, so reruns cannot
duplicate rows.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import psycopg

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.gold import qec_syndromes
from quantum_lake_student.models import StageResult
from quantum_lake_student.sources.qec_syndromes import DEFAULT_RESULTS_DIR

# source name -> loader(connection, settings) returning {gold table: row count}.
# Teammates: register the google_qec and QASMBench Gold loaders here once merged.
GOLD_LOADERS: dict[str, Callable[[psycopg.Connection, Settings], dict[str, int]]] = {
    qec_syndromes.SOURCE_NAME: qec_syndromes.load_from_lake,
}


def write_row_counts(counts: dict[str, int], results_dir: Path) -> None:
    """Update our tables in the "gold" section of the shared row_counts.json."""
    path = results_dir / "row_counts.json"
    existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    existing.setdefault("gold", {}).update(counts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path = DEFAULT_RESULTS_DIR,
) -> StageResult:
    if settings is None:
        settings = Settings.from_environment()
    result = StageResult(stage="gold", run_id=run_id)

    counts: dict[str, int] = {}
    with postgres_connection(settings) as connection:
        with connection.transaction():
            for load in GOLD_LOADERS.values():
                counts.update(load(connection, settings))
    write_row_counts(counts, results_dir)

    result.output_count = sum(counts.values())
    result.finish()
    return result


if __name__ == "__main__":
    stage = run("manual")
    print(f"{stage.stage}: loaded {stage.output_count} rows into schema gold")
