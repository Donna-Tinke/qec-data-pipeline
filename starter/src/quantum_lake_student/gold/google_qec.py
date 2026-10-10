"""Load the Google QEC Silver tables into the PostgreSQL Gold model.

Design and rationale: ``starter/docs/gold-google-qec-design.md``.

The whole load (DDL, data, derived tables, views, checks) runs in a single
transaction: it either completes or the previous Gold version is untouched.
"""

from __future__ import annotations

import json
import sys
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
import pyarrow as pa
import pyarrow.parquet as pq

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.lake import read_parquet
from quantum_lake_student.ml import (
    MODEL_SPLITS,
    google_data_split,
    unpack_little_endian_bits,
)
from quantum_lake_student.models import StageResult

GOLD_SCHEMA = "gold"
SQL_DIR = Path(__file__).parent / "sql" / "google_qec"
ML_OBJECT = "ml/ml_google_decoder_example.parquet"
BATCH_ROWS = 10_000

ML_SCHEMA = pa.schema(
    [
        ("example_id", pa.string()),
        ("experiment_id", pa.string()),
        ("shot_index", pa.int64()),
        ("distance", pa.int32()),
        ("rounds", pa.int32()),
        ("center_row", pa.int32()),
        ("center_col", pa.int32()),
        ("detector_count", pa.int32()),
        ("detector_event_count", pa.int32()),
        ("detector_bits", pa.binary()),
        ("belief_matching_prediction", pa.bool_()),
        ("correlated_matching_prediction", pa.bool_()),
        ("pymatching_prediction", pa.bool_()),
        ("tensor_network_contraction_prediction", pa.bool_()),
        ("actual_observable_flip", pa.bool_()),
        ("data_split", pa.string()),
    ]
)

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


class ContractError(ValueError):
    """The exported ML table does not match its required contract."""


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


def read_silver_tables(settings: Settings) -> tuple[pa.Table, pa.Table]:
    """Read the experiment and shot Silver Parquet tables from the lake in memory."""
    return (
        read_parquet("silver/google_qec/experiment.parquet", settings),
        read_parquet("silver/google_qec/shot.parquet", settings),
    )


def _copy_parquet(
    cursor: psycopg.Cursor,
    source: pa.Table,
    table: str,
    columns: tuple[tuple[str, str], ...],
) -> int:
    col_names = [name for name, _ in columns]
    names = ", ".join(col_names)
    rows = 0
    with cursor.copy(f"COPY {table} ({names}) FROM STDIN (FORMAT BINARY)") as copy:
        copy.set_types([pg_type for _, pg_type in columns])
        for batch in source.select(col_names).to_batches(max_chunksize=BATCH_ROWS):
            for row in zip(*(batch.column(name).to_pylist() for name in col_names)):
                copy.write_row(row)
            rows += batch.num_rows
    return rows


def load_gold(
    connection: psycopg.Connection,
    experiments_table: pa.Table,
    shots_table: pa.Table,
    *,
    schema: str = GOLD_SCHEMA,
) -> dict[str, int]:
    """Replace the google_qec Gold objects in `schema` atomically. Returns row counts."""
    shot_columns = ", ".join(f"{name} {pg_type}" for name, pg_type in SHOT_STAGING_COLUMNS)
    prediction_values = ", ".join(
        f"({decoder_id}, s.{column})" for decoder_id, column in DECODER_COLUMNS
    )
    with connection.transaction():  # one transaction: commit all or roll back all
        connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        previous_search_path = connection.execute("SHOW search_path").fetchone()[0]
        connection.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))

        cursor = connection.cursor()
        cursor.execute(_sql("01_schema.sql"))

        experiments = _copy_parquet(
            cursor, experiments_table, "google_experiment", EXPERIMENT_COLUMNS
        )
        cursor.execute("DROP TABLE IF EXISTS pg_temp.stg_shot")
        cursor.execute(f"CREATE TEMP TABLE stg_shot ({shot_columns}) ON COMMIT DROP")
        staged = _copy_parquet(cursor, shots_table, "stg_shot", SHOT_STAGING_COLUMNS)

        cursor.execute(
            """
            INSERT INTO google_shot
                (experiment_id, shot_index, source_record_id, detector_bits,
                 detector_event_count, actual_observable_flip)
            SELECT experiment_id, shot_index, source_record_id, detector_bits,
                   detector_event_count, actual_observable_flip
            FROM stg_shot"""
        )
        cursor.execute(
            """
            INSERT INTO google_shot_raw
                (experiment_id, shot_index, measurement_bits, sweep_bits)
            SELECT experiment_id, shot_index, measurement_bits, sweep_bits FROM stg_shot"""
        )
        cursor.execute(
            f"""
            INSERT INTO google_shot_prediction
                (experiment_id, shot_index, decoder_id, predicted_flip)
            SELECT s.experiment_id, s.shot_index, v.decoder_id, v.predicted_flip
            FROM stg_shot s
            CROSS JOIN LATERAL (VALUES {prediction_values}) AS v(decoder_id, predicted_flip)"""
        )
        bad_length = cursor.execute(
            """
            SELECT count(*) FROM google_shot s
            JOIN google_experiment e USING (experiment_id)
            WHERE octet_length(s.detector_bits) <> (e.detector_count + 7) / 8"""
        ).fetchone()[0]
        if bad_length:  # must precede get_bit(), which fails obscurely on short rows
            raise GoldLoadError(f"{bad_length} shots have a wrong detector_bits byte length")
        # bytea bits are numbered like Stim b8 (bit i = byte i/8, bit i%8 from the LSB)
        cursor.execute(
            """
            INSERT INTO google_detector_position_stat
                (experiment_id, detector_index, shots_observed, fired_count)
            SELECT s.experiment_id, g.i, count(*), sum(get_bit(s.detector_bits, g.i))
            FROM google_shot s
            JOIN google_experiment e USING (experiment_id)
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
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0]

        connection.execute("SELECT set_config('search_path', %s, true)", (previous_search_path,))
    return counts


# ---------------------------------------------------------------- Gold -> ML


def fetch_ml_examples(connection: psycopg.Connection) -> pa.Table:
    """Read ml_google_decoder_example from the Gold view; only data_split is added here."""
    rows = connection.execute(
        """
        SELECT example_id, experiment_id, shot_index, distance, rounds,
               center_row, center_col, detector_count, detector_event_count,
               detector_bits, belief_matching_prediction,
               correlated_matching_prediction, pymatching_prediction,
               tensor_network_contraction_prediction, actual_observable_flip
        FROM v_ml_google_decoder_example
        ORDER BY experiment_id, shot_index
        """
    ).fetchall()
    names = ML_SCHEMA.names[:-1]
    records = [dict(zip(names, row)) for row in rows]
    for record in records:
        record["detector_bits"] = bytes(record["detector_bits"])
        record["data_split"] = google_data_split(record["shot_index"])

    table = pa.Table.from_pylist(records, schema=ML_SCHEMA)
    gold_rows = connection.execute("SELECT count(*) FROM google_shot").fetchone()[0]
    if table.num_rows != gold_rows:
        raise ContractError(f"view returned {table.num_rows} rows, Gold has {gold_rows} shots")
    return table


def contract_problems(table: pa.Table) -> list[str]:
    """Checks from assignment/required-ml-tables.md; empty list = contract met."""
    if table.schema != ML_SCHEMA:
        return [f"schema differs from the contract: {table.schema}"]
    problems = []
    rows = table.to_pylist()

    example_ids = [row["example_id"] for row in rows]
    if len(set(example_ids)) != len(example_ids):
        problems.append("example_id is not unique")
    if any(value is None for row in rows for value in row.values()):
        problems.append("null values present")
    missing_splits = set(MODEL_SPLITS) - {row["data_split"] for row in rows}
    if missing_splits:
        problems.append(f"empty splits: {sorted(missing_splits)}")

    prediction_columns = (
        "belief_matching_prediction",
        "correlated_matching_prediction",
        "pymatching_prediction",
        "tensor_network_contraction_prediction",
    )

    for row in rows:
        prefix = f"example {row['example_id']}"
        detector_count = row["detector_count"]
        detector_bits = row["detector_bits"] or b""
        expected_bytes = (detector_count + 7) // 8
        if len(detector_bits) != expected_bytes:
            problems.append(f"{prefix}: wrong detector_bits length {len(detector_bits)} != {expected_bytes}")
        else:
            remainder = detector_count % 8
            if remainder != 0 and (detector_bits[-1] >> remainder) != 0:
                problems.append(f"{prefix}: padding bits are not zero")
            unpacked = unpack_little_endian_bits(detector_bits, detector_count)
            if sum(unpacked) != row["detector_event_count"]:
                problems.append(f"{prefix}: detector_event_count mismatch")

        if row["data_split"] != google_data_split(row["shot_index"]):
            problems.append(f"{prefix}: data_split does not match the course split")
        if row["distance"] == 3 and detector_count != 200:
            problems.append(f"{prefix}: distance 3 must have 200 detectors")
        if row["distance"] == 5 and detector_count != 600:
            problems.append(f"{prefix}: distance 5 must have 600 detectors")
        for column in prediction_columns:
            if not isinstance(row[column], bool):
                problems.append(f"{prefix}: invalid {column}")
        if not isinstance(row["actual_observable_flip"], bool):
            problems.append(f"{prefix}: invalid actual_observable_flip")
        if len(problems) > 20:
            problems.append("... more problems omitted")
            break
    return problems


def export_ml_examples(connection: psycopg.Connection) -> pa.Table:
    table = fetch_ml_examples(connection)
    problems = contract_problems(table)
    if problems:
        raise ContractError("ml_google_decoder_example: " + "; ".join(problems))
    return table


# ---------------------------------------------------------------- analysis and tracing


def run_analyses(connection: psycopg.Connection, out_dir: Path) -> list[Path]:
    """Run the committed analysis SQL and detector-storage comparison, storing each result as JSON."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for sql_path in sorted((SQL_DIR / "analysis").glob("*.sql")):
        cursor = connection.execute(sql_path.read_text())
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor.fetchall()]
        target = out_dir / f"{sql_path.stem}.json"
        target.write_text(json.dumps({"query": sql_path.name, "rows": rows}, indent=2, default=str))
        written.append(target)
    storage = measure_detector_storage(connection)
    storage_target = out_dir / "google_detector_storage.json"
    storage_target.write_text(json.dumps(storage, indent=2))
    written.append(storage_target)
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
            FROM google_shot s
            JOIN google_experiment e USING (experiment_id)
            CROSS JOIN LATERAL generate_series(0, e.detector_count - 1) AS g(i)
            WHERE s.shot_index < %s AND get_bit(s.detector_bits, g.i) = 1""",
            (sample_shots,),
        )
        sample_events, sampled_shots = connection.execute(
            """
            SELECT (SELECT count(*) FROM detector_event_sample),
                   (SELECT count(*) FROM google_shot WHERE shot_index < %s)""",
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
            FROM google_shot s JOIN google_experiment e USING (experiment_id)"""
        ).fetchone()
        table_sizes = dict(
            connection.execute(
                """
                SELECT c.relname, pg_total_relation_size(c.oid)
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
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


def run(
    run_id: str,
    settings: Settings | None = None,
    connection: psycopg.Connection | None = None,
    *,
    schema: str = GOLD_SCHEMA,
) -> StageResult:
    if settings is None:
        settings = Settings.from_environment()
    result = StageResult(stage="gold.google_qec", run_id=run_id)

    experiments_table, shots_table = read_silver_tables(settings)
    conn_ctx = nullcontext(connection) if connection is not None else postgres_connection(settings)
    with conn_ctx as conn:
        counts = load_gold(conn, experiments_table, shots_table, schema=schema)
    result.input_count = counts["silver_shots"]
    result.output_count = counts["gold.google_shot"]
    result.table_counts = {
        k: v for k, v in counts.items() if k.startswith("gold.")
    }
    result.finish()
    return result


def trace_example(connection: psycopg.Connection, results_dir: Path) -> dict[str, Any]:
    """Follow one Google ML example back through Gold and Silver to its Bronze companion files.

    Harmonized with the 5-tier lineage schema (ml -> gold -> silver -> bronze -> prediction).
    One Google shot is composed of 8 companion files in Bronze:
      - measurements.b8 (measurement bits)
      - sweep.b8 (sweep parameters)
      - detection_events.b8 (detector events)
      - obs_flips_actual.01 (actual flip labels)
      - 4 decoder predictions (.01 files)
    """
    import pyarrow.compute as pc

    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT s.example_id, s.experiment_id, s.shot_index,
               e.source_record_id AS experiment_source_record_id,
               e.basis, e.distance, e.rounds, e.shots,
               e.center_row, e.center_col, e.measurement_count, e.detector_count,
               s.detector_bits, s.detector_event_count, s.actual_observable_flip,
               r.measurement_bits, r.sweep_bits,
               s.belief_matching_prediction, s.correlated_matching_prediction,
               s.pymatching_prediction, s.tensor_network_contraction_prediction
        FROM v_ml_google_decoder_example s
        JOIN google_experiment e USING (experiment_id)
        JOIN google_shot_raw r USING (experiment_id, shot_index)
        WHERE s.shot_index % 2 = 1
        ORDER BY s.shot_index
        LIMIT 1
        """
    )
    col_names = [d[0] for d in cursor.description]
    row = cursor.fetchone()
    if not row:
        return {}

    data = dict(zip(col_names, row))
    example_id = data["example_id"]
    experiment_id = data["experiment_id"]
    shot_index = data["shot_index"]
    source_record_id = f"google_qec:{experiment_id}:shot={shot_index}"

    cursor.execute(
        """
        SELECT p.experiment_id, p.shot_index, p.decoder_id, d.decoder_name, p.predicted_flip
        FROM google_shot_prediction p
        JOIN decoder d USING (decoder_id)
        WHERE p.experiment_id = %s AND p.shot_index = %s
        ORDER BY p.decoder_id
        """,
        (experiment_id, shot_index),
    )
    prediction_rows = [
        {
            "experiment_id": r[0],
            "shot_index": r[1],
            "decoder_id": r[2],
            "decoder_name": r[3],
            "predicted_flip": bool(r[4]),
        }
        for r in cursor.fetchall()
    ]

    companion_traces: list[dict[str, Any]] = []
    candidates = [
        results_dir / "source_trace.parquet",
        Path("results/part1/source_trace.parquet"),
    ]
    for trace_path in candidates:
        if trace_path.exists():
            try:
                table = pq.read_table(trace_path)
                g_mask = pc.and_(
                    pc.equal(table["source_name"], "google_qec"),
                    pc.equal(table["source_record_id"], source_record_id),
                )
                companion_traces = table.filter(g_mask).to_pylist()
                if companion_traces:
                    break
            except Exception:
                continue

    return {
        "ml": {
            "table": ML_OBJECT,
            "example_id": example_id,
            "data_split": google_data_split(shot_index),
            "actual_observable_flip": bool(data["actual_observable_flip"]),
            "detector_event_count": data["detector_event_count"],
        },
        "gold": {
            "v_google_example_lookup": {
                "example_id": example_id,
                "experiment_id": experiment_id,
                "shot_index": shot_index,
                "source_record_id": source_record_id,
            },
            "google_shot": {
                "experiment_id": experiment_id,
                "shot_index": shot_index,
                "detector_bits_hex": bytes(data["detector_bits"]).hex(),
                "detector_event_count": data["detector_event_count"],
                "actual_observable_flip": bool(data["actual_observable_flip"]),
                "source_record_id": source_record_id,
            },
            "google_experiment": {
                "experiment_id": experiment_id,
                "source_record_id": data["experiment_source_record_id"],
                "basis": data["basis"],
                "distance": data["distance"],
                "rounds": data["rounds"],
                "shots": data["shots"],
                "center_row": data["center_row"],
                "center_col": data["center_col"],
                "measurement_count": data["measurement_count"],
                "detector_count": data["detector_count"],
            },
            "google_shot_raw": {
                "experiment_id": experiment_id,
                "shot_index": shot_index,
                "measurement_bits_hex": bytes(data["measurement_bits"]).hex(),
                "sweep_bits_hex": bytes(data["sweep_bits"]).hex(),
            },
            "google_shot_prediction": prediction_rows,
        },
        "silver": {
            "shot": {
                "table": "silver/google_qec/shot.parquet",
                "source_record_id": source_record_id,
            },
            "experiment": {
                "table": "silver/google_qec/experiment.parquet",
                "source_record_id": data["experiment_source_record_id"],
            },
        },
        "bronze": companion_traces,
        "prediction": "results/part2/predictions.parquet rows with this example_id",
    }


if __name__ == "__main__":
    print(run(sys.argv[1] if len(sys.argv) > 1 else "manual"))
