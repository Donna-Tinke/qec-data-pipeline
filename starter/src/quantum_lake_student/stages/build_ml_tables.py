"""Create the required analyses and ML input tables from PostgreSQL Gold.

Write the Gold queries/views, joins, stable example IDs, and export logic.
A thin export step may execute SQL, use the course split helpers, validate the
fixed contracts, and write Parquet. It must not re-read Bronze or Silver.

All values come from Gold views/queries (sql/gold/*.sql, sql/analysis/*.sql).
This step only adds data_split with the course helper, checks the contract and
writes Parquet. Teammates: add the google_qec export next to the syndrome one.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg import sql

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.gold import qec_syndromes
from quantum_lake_student.lake import write_parquet
from quantum_lake_student.models import StageResult
from quantum_lake_student.sources.qec_syndromes import DEFAULT_RESULTS_DIR


def use_gold(connection: psycopg.Connection, schema: str = qec_syndromes.GOLD_SCHEMA) -> None:
    connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path = DEFAULT_RESULTS_DIR,
) -> StageResult:
    if settings is None:
        settings = Settings.from_environment()
    result = StageResult(stage="ml", run_id=run_id)

    with postgres_connection(settings) as connection:
        use_gold(connection)
        syndrome_examples = qec_syndromes.export_ml_examples(connection)

    write_parquet(syndrome_examples, qec_syndromes.ML_OBJECT, settings)

    result.output_count = syndrome_examples.num_rows
    result.finish()
    return result


if __name__ == "__main__":
    stage = run("manual")
    print(f"{stage.stage}: exported {stage.output_count} ML rows")
