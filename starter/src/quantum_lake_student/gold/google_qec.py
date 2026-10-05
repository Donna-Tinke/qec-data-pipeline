"""Load the Google QEC Silver tables into the PostgreSQL Gold model.

Design and rationale: ``starter/docs/gold-google-qec-design.md``.

The whole load (DDL, data, derived tables, views, checks) runs in a single
transaction: it either completes or the previous Gold version is untouched.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import psycopg
import pyarrow.parquet as pq

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.models import StageResult

SQL_DIR = Path(__file__).parent / "sql" / "google_qec"
BATCH_ROWS = 10_000

# (Silver column, PostgreSQL type) in the order of the staging table.
SHOT_STAGING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_record_id", "text"),
    ("experiment_id", "text"),
    ("shot_index", "int8"),
    ("measurement_bits", "bytea"),
    ("sweep_bits", "bytea"),
    ("detector_bits", "bytea"),
    ("detector_event_count", "int4"),
    ("actual_observable_flip", "bool"),
    ("belief_matching_prediction", "bool"),
    ("correlated_matching_prediction", "bool"),
    ("pymatching_prediction", "bool"),
    ("tensor_network_contraction_prediction", "bool"),
)
EXPERIMENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("experiment_id", "text"),
    ("source_record_id", "text"),
    ("basis", "text"),
    ("distance", "int4"),
    ("rounds", "int4"),
    ("shots", "int8"),
    ("center_row", "int4"),
    ("center_col", "int4"),
    ("measurement_count", "int4"),
    ("detector_count", "int4"),
)
# decoder_id (see 01_schema.sql) -> staging column
DECODER_COLUMNS: tuple[tuple[int, str], ...] = (
    (1, "belief_matching_prediction"),
    (2, "correlated_matching_prediction"),
    (3, "pymatching_prediction"),
    (4, "tensor_network_contraction_prediction"),
)


class GoldLoadError(RuntimeError):
    """A post-load check failed; the transaction was rolled back."""


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


def silver_dir(settings: Settings) -> Path:
    return settings.local_lake_root / "silver" / "google_qec"


def _copy_parquet(
    cursor: psycopg.Cursor, path: Path, table: str, columns: tuple[tuple[str, str], ...]
) -> int:
    names = ", ".join(name for name, _ in columns)
    rows = 0
    with cursor.copy(f"COPY {table} ({names}) FROM STDIN (FORMAT BINARY)") as copy:
        copy.set_types([pg_type for _, pg_type in columns])
        for batch in pq.ParquetFile(path).iter_batches(
            batch_size=BATCH_ROWS, columns=[name for name, _ in columns]
        ):
            for row in zip(*(batch.column(name).to_pylist() for name, _ in columns)):
                copy.write_row(row)
            rows += batch.num_rows
    return rows


def load_gold(connection: psycopg.Connection, silver: Path) -> dict[str, int]:
    """Replace the google_qec Gold objects atomically. Returns row counts."""
    shot_columns = ", ".join(f"{name} {pg_type}" for name, pg_type in SHOT_STAGING_COLUMNS)
    prediction_values = ", ".join(
        f"({decoder_id}, s.{column})" for decoder_id, column in DECODER_COLUMNS
    )
    with connection.transaction():  # one transaction: commit all or roll back all
        cursor = connection.cursor()
        cursor.execute(_sql("01_schema.sql"))

        experiments = _copy_parquet(
            cursor, silver / "experiment.parquet", "gold.google_experiment", EXPERIMENT_COLUMNS
        )
        cursor.execute("DROP TABLE IF EXISTS pg_temp.stg_shot")
        cursor.execute(f"CREATE TEMP TABLE stg_shot ({shot_columns}) ON COMMIT DROP")
        staged = _copy_parquet(cursor, silver / "shot.parquet", "stg_shot", SHOT_STAGING_COLUMNS)

        cursor.execute(
            """
            INSERT INTO gold.google_shot
                (experiment_id, shot_index, source_record_id, detector_bits,
                 detector_event_count, actual_observable_flip)
            SELECT experiment_id, shot_index, source_record_id, detector_bits,
                   detector_event_count, actual_observable_flip
            FROM stg_shot"""
        )
        cursor.execute(
            """
            INSERT INTO gold.google_shot_raw
                (experiment_id, shot_index, measurement_bits, sweep_bits)
            SELECT experiment_id, shot_index, measurement_bits, sweep_bits FROM stg_shot"""
        )
        cursor.execute(
            f"""
            INSERT INTO gold.google_shot_prediction
                (experiment_id, shot_index, decoder_id, predicted_flip)
            SELECT s.experiment_id, s.shot_index, v.decoder_id, v.predicted_flip
            FROM stg_shot s
            CROSS JOIN LATERAL (VALUES {prediction_values}) AS v(decoder_id, predicted_flip)"""
        )
        bad_length = cursor.execute(
            """
            SELECT count(*) FROM gold.google_shot s
            JOIN gold.google_experiment e USING (experiment_id)
            WHERE octet_length(s.detector_bits) <> (e.detector_count + 7) / 8"""
        ).fetchone()[0]
        if bad_length:  # must precede get_bit(), which fails obscurely on short rows
            raise GoldLoadError(f"{bad_length} shots have a wrong detector_bits byte length")
        # bytea bits are numbered like Stim b8 (bit i = byte i/8, bit i%8 from the LSB)
        cursor.execute(
            """
            INSERT INTO gold.google_detector_position_stat
                (experiment_id, detector_index, shots_observed, fired_count)
            SELECT s.experiment_id, g.i, count(*), sum(get_bit(s.detector_bits, g.i))
            FROM gold.google_shot s
            JOIN gold.google_experiment e USING (experiment_id)
            CROSS JOIN LATERAL generate_series(0, e.detector_count - 1) AS g(i)
            GROUP BY s.experiment_id, g.i"""
        )

        cursor.execute(_sql("02_views.sql"))
        cursor.execute(_sql("ml_google_decoder_example.sql"))

        failures = cursor.execute(_sql("03_checks.sql")).fetchall()
        if failures:
            raise GoldLoadError(f"Gold checks failed, load rolled back: {failures}")

        counts = {
            "silver_experiments": experiments,
            "silver_shots": staged,
        }
        for table in (
            "decoder",
            "google_experiment",
            "google_shot",
            "google_shot_raw",
            "google_shot_prediction",
            "google_detector_position_stat",
        ):
            counts[f"gold.{table}"] = cursor.execute(
                f"SELECT count(*) FROM gold.{table}"
            ).fetchone()[0]
    return counts


def run_analyses(connection: psycopg.Connection, out_dir: Path) -> list[Path]:
    """Run the committed analysis SQL and store each result as JSON."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for sql_path in sorted((SQL_DIR / "analysis").glob("*.sql")):
        cursor = connection.execute(sql_path.read_text())
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor.fetchall()]
        target = out_dir / f"{sql_path.stem}.json"
        target.write_text(json.dumps({"query": sql_path.name, "rows": rows}, indent=2, default=str))
        written.append(target)
    return written


def measure_detector_storage(
    connection: psycopg.Connection, sample_shots: int = 2_000
) -> dict[str, Any]:
    """Compare detector-storage strategies; returns sizes in bytes.

    The one-row-per-fired-detector alternative is materialised only for the
    first ``sample_shots`` shots of every experiment and extrapolated, to avoid
    building millions of rows just to measure them.
    """
    with connection.transaction():
        connection.execute("DROP TABLE IF EXISTS pg_temp.detector_event_sample")
        connection.execute(
            """
            CREATE TEMP TABLE detector_event_sample (
                experiment_id text, shot_index bigint, detector_index integer,
                PRIMARY KEY (experiment_id, shot_index, detector_index))"""
        )
        connection.execute(
            """
            INSERT INTO detector_event_sample
            SELECT s.experiment_id, s.shot_index, g.i
            FROM gold.google_shot s
            JOIN gold.google_experiment e USING (experiment_id)
            CROSS JOIN LATERAL generate_series(0, e.detector_count - 1) AS g(i)
            WHERE s.shot_index < %s AND get_bit(s.detector_bits, g.i) = 1""",
            (sample_shots,),
        )
        sample_events, sampled_shots = connection.execute(
            """
            SELECT (SELECT count(*) FROM detector_event_sample),
                   (SELECT count(*) FROM gold.google_shot WHERE shot_index < %s)""",
            (sample_shots,),
        ).fetchone()
        sample_bytes = connection.execute(
            "SELECT pg_total_relation_size('detector_event_sample')"
        ).fetchone()[0]
        total_shots, total_events, packed_column_bytes, packed_bytes, unpacked_bytes = connection.execute(
            """
            SELECT count(*), sum(detector_event_count),
                   sum(pg_column_size(detector_bits)), sum(octet_length(detector_bits)),
                   sum(e.detector_count)
            FROM gold.google_shot s JOIN gold.google_experiment e USING (experiment_id)"""
        ).fetchone()
        table_sizes = dict(
            connection.execute(
                """
                SELECT c.relname, pg_total_relation_size(c.oid)
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'gold'
                  AND c.relname IN ('google_shot', 'google_shot_raw',
                                    'google_shot_prediction', 'google_detector_position_stat')"""
            ).fetchall()
        )
        connection.execute("DROP TABLE detector_event_sample")
    scale = total_shots / sampled_shots
    return {
        "shots_total": total_shots,
        "detector_events_total": int(total_events),
        "strategies": {
            "packed_bytes_in_shot_row": {
                "detector_bits_column_bytes": int(packed_column_bytes),
                "google_shot_table_incl_indexes_bytes": table_sizes["google_shot"],
                "reproduces_ml_packed_bytes": True,
            },
            "per_position_summary": {
                "rows": "sum(detector_count) per experiment",
                "table_incl_indexes_bytes": table_sizes["google_detector_position_stat"],
                "reproduces_ml_packed_bytes": False,
            },
            "one_row_per_fired_detector": {
                "sample_shots": int(sampled_shots),
                "sample_rows": int(sample_events),
                "sample_bytes": int(sample_bytes),
                "extrapolated_rows": int(sample_events * scale),
                "extrapolated_bytes": int(sample_bytes * scale),
                "reproduces_ml_packed_bytes": True,
            },
        },
        "unpacked_one_byte_per_bit_bytes": int(unpacked_bytes),
    }


def run(run_id: str) -> StageResult:
    settings = Settings.from_environment()
    result = StageResult(stage="gold.google_qec", run_id=run_id)
    results_dir = settings.local_lake_root / "results" / "part1"
    with postgres_connection(settings) as connection:
        counts = load_gold(connection, silver_dir(settings))
        run_analyses(connection, results_dir / "analysis")
        storage = measure_detector_storage(connection)
    (results_dir / "analysis" / "google_detector_storage.json").write_text(
        json.dumps(storage, indent=2)
    )
    (results_dir / "gold_google_qec_counts.json").write_text(json.dumps(counts, indent=2))
    result.input_count = counts["silver_shots"]
    result.output_count = counts["gold.google_shot"]
    result.finish()
    return result


if __name__ == "__main__":
    print(run(sys.argv[1] if len(sys.argv) > 1 else "manual"))
