"""Gold load and Gold-to-ML export for the qec_syndromes source.

Gold tables (sql/qec_syndromes/01_schema.sql):
  simulated_experiment  one row per fault-rate file          (7)
  syndrome_pattern      one row per distinct 16-bit syndrome (~32k)
  syndrome_observation  one row per accepted Silver row      (75,598)

load_gold() rebuilds only these objects inside schema "gold", in one
transaction, like the google_qec and QASMBench loaders do. Any failure rolls
the whole load back, so the previous Gold version stays in place.
"""

from __future__ import annotations

from collections import Counter
from math import isclose
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
from psycopg import sql

from quantum_lake_student.config import Settings
from quantum_lake_student.ml import MODEL_SPLITS, syndrome_data_split, syndrome_model_input
from quantum_lake_student.sources import qec_syndromes as silver

SOURCE_NAME = silver.SOURCE_NAME
GOLD_SCHEMA = "gold"
SQL_DIR = Path(__file__).parent / "sql" / SOURCE_NAME
ML_OBJECT = "ml/ml_syndrome_decoder_example.parquet"


class GoldLoadError(RuntimeError):
    """A Silver-to-Gold check failed; the load transaction was rolled back."""


class ContractError(ValueError):
    """The exported ML table does not match its required contract."""


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


ML_SCHEMA = pa.schema(
    [
        ("example_id", pa.string()),
        ("experiment_id", pa.string()),
        ("physical_fault_rate", pa.float64()),
        ("syndrome_bits", pa.binary()),
        ("round_count", pa.int32()),
        ("check_count", pa.int32()),
        ("logical_error_label", pa.bool_()),
        ("sample_weight", pa.int64()),
        ("data_split", pa.string()),
    ]
)


# ---------------------------------------------------------------- Gold load


def experiment_rows(observations: pa.Table) -> list[dict[str, Any]]:
    """One row per experiment, combining Silver values with the filename metadata.

    experiment_id is the CSV filename stem, so distance and the nominal shot
    count ("nb-10M") come from parsing it again with the Silver parser.
    """
    per_experiment: dict[str, set[tuple[float, int, int]]] = {}
    columns = observations.select(
        ["experiment_id", "physical_fault_rate", "round_count", "check_count"]
    ).to_pylist()
    for row in columns:
        per_experiment.setdefault(row["experiment_id"], set()).add(
            (row["physical_fault_rate"], row["round_count"], row["check_count"])
        )

    rows = []
    for experiment_id, values in sorted(per_experiment.items()):
        if len(values) != 1:
            raise GoldLoadError(
                f"{experiment_id}: Silver rows disagree on fault rate/shape: {sorted(values)}"
            )
        fault_rate, round_count, check_count = next(iter(values))
        metadata = silver.parse_filename(f"{experiment_id}.csv")
        if metadata is None:
            raise GoldLoadError(f"{experiment_id}: experiment_id is not a source filename stem")
        if not isclose(metadata.physical_fault_rate, fault_rate, rel_tol=0.0, abs_tol=1e-12):
            raise GoldLoadError(
                f"{experiment_id}: filename fault rate {metadata.physical_fault_rate} "
                f"!= Silver value {fault_rate}"
            )
        rows.append(
            {
                "experiment_id": experiment_id,
                "distance": metadata.distance,
                "round_count": round_count,
                "physical_fault_rate": fault_rate,
                "check_count": check_count,
                "nominal_shot_count": metadata.nominal_sample_count,
            }
        )
    return rows


def check_unique_keys(observations: pa.Table) -> None:
    """Silver keeps a repeated (experiment, syndrome, label) row with a warning;
    Gold's primary key cannot hold it, so stop with a readable message."""
    keys = observations.select(["experiment_id", "syndrome_bits", "logical_error_label"]).to_pylist()
    counts = Counter(tuple(key.values()) for key in keys)
    repeated = [key for key, count in counts.items() if count > 1]
    if repeated:
        experiment_id, bits, label = repeated[0]
        raise GoldLoadError(
            f"{len(repeated)} repeated (experiment, syndrome, label) keys in Silver, "
            f"e.g. {experiment_id} {bits.hex()} label={label}; decide how Gold should combine them"
        )


def load_gold(
    connection: psycopg.Connection, observations: pa.Table, *, schema: str = GOLD_SCHEMA
) -> dict[str, int]:
    """Replace the qec_syndromes Gold objects in `schema` atomically. Returns row counts."""
    check_unique_keys(observations)
    experiments = experiment_rows(observations)

    with connection.transaction():  # one transaction: commit all or roll back all
        connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        previous_search_path = connection.execute("SHOW search_path").fetchone()[0]
        # SET LOCAL ends with the transaction; restored below in case we run nested
        connection.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
        connection.execute(_sql("01_schema.sql"))

        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO simulated_experiment (experiment_id, distance, round_count, check_count, "
                "physical_fault_rate, nominal_shot_count) VALUES (%(experiment_id)s, %(distance)s, "
                "%(round_count)s, %(check_count)s, %(physical_fault_rate)s, %(nominal_shot_count)s)",
                experiments,
            )

            cursor.execute("DROP TABLE IF EXISTS pg_temp.syndrome_observation_load")
            cursor.execute(
                "CREATE TEMP TABLE syndrome_observation_load ("
                " source_record_id text, experiment_id text, syndrome_bits bytea,"
                " logical_error_label boolean, quantity bigint"
                ") ON COMMIT DROP"
            )
            load_columns = ["source_record_id", "experiment_id", "syndrome_bits", "logical_error_label", "quantity"]
            with cursor.copy(f"COPY syndrome_observation_load ({', '.join(load_columns)}) FROM STDIN") as copy:
                for row in observations.select(load_columns).to_pylist():
                    copy.write_row([row[name] for name in load_columns])

            # ids follow byte order of the pattern, so a rebuild on the same data gives the same ids
            cursor.execute(
                "INSERT INTO syndrome_pattern (syndrome_id, syndrome_bits) "
                "SELECT row_number() OVER (ORDER BY syndrome_bits), syndrome_bits "
                "FROM (SELECT DISTINCT syndrome_bits FROM syndrome_observation_load) AS distinct_bits"
            )
            cursor.execute(
                "INSERT INTO syndrome_observation "
                "(experiment_id, syndrome_id, logical_error_label, quantity, observation_id, source_record_id) "
                "SELECT l.experiment_id, p.syndrome_id, l.logical_error_label, l.quantity, "
                "       syndrome_observation_id(l.experiment_id, l.syndrome_bits, l.logical_error_label), "
                "       l.source_record_id "
                "FROM syndrome_observation_load l JOIN syndrome_pattern p USING (syndrome_bits)"
            )

        counts = check_load(connection, observations, experiments)
        connection.execute("SELECT set_config('search_path', %s, true)", (previous_search_path,))
    return counts


def check_load(
    connection: psycopg.Connection, observations: pa.Table, experiments: list[dict[str, Any]]
) -> dict[str, int]:
    """Silver-to-Gold reconciliation; raising here aborts the whole Gold load."""
    counts = {
        "simulated_experiment": _count(connection, "simulated_experiment"),
        "syndrome_pattern": _count(connection, "syndrome_pattern"),
        "syndrome_observation": _count(connection, "syndrome_observation"),
    }
    expected = {
        "simulated_experiment": len(experiments),
        "syndrome_pattern": len(set(observations.column("syndrome_bits").to_pylist())),
        "syndrome_observation": observations.num_rows,
    }
    if counts != expected:
        raise GoldLoadError(f"Silver-to-Gold counts do not reconcile: gold={counts} expected={expected}")

    # Compare with what Silver accepted, not with the filename's nominal count:
    # Silver already reports a total that misses "nb-10M" as a kept warning, and
    # a row Silver rejected must not block the whole Gold load.
    silver_totals: Counter[str] = Counter()
    for row in observations.select(["experiment_id", "quantity"]).to_pylist():
        silver_totals[row["experiment_id"]] += row["quantity"]
    gold_totals = dict(
        connection.execute(
            "SELECT experiment_id, sum(quantity)::bigint FROM syndrome_observation GROUP BY experiment_id"
        ).fetchall()
    )
    if gold_totals != dict(silver_totals):
        raise GoldLoadError(
            f"weighted totals do not reconcile: gold={gold_totals} silver={dict(silver_totals)}"
        )
    return counts


def _count(connection: psycopg.Connection, table: str) -> int:
    # table names are fixed constants from this module, never user input
    return connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def load_from_lake(connection: psycopg.Connection, settings: Settings) -> dict[str, int]:
    return load_gold(connection, silver.read_silver_table(settings))


# ---------------------------------------------------------------- Gold -> ML


def fetch_ml_examples(connection: psycopg.Connection) -> pa.Table:
    """Read ml_syndrome_decoder_example from the Gold view; only data_split is added here."""
    rows = connection.execute(
        "SELECT example_id, experiment_id, physical_fault_rate, syndrome_bits, round_count, "
        "check_count, logical_error_label, sample_weight "
        "FROM v_ml_syndrome_decoder_example ORDER BY example_id"
    ).fetchall()
    names = ML_SCHEMA.names[:-1]
    records = [dict(zip(names, row)) for row in rows]
    for record in records:
        record["syndrome_bits"] = bytes(record["syndrome_bits"])
        record["data_split"] = syndrome_data_split(record["physical_fault_rate"])

    table = pa.Table.from_pylist(records, schema=ML_SCHEMA)
    gold_rows = _count(connection, "syndrome_observation")
    if table.num_rows != gold_rows:
        raise ContractError(f"view returned {table.num_rows} rows, Gold has {gold_rows} observations")
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

    for row in rows:
        prefix = f"example {row['example_id']}"
        bits = row["syndrome_bits"] or b""
        try:
            syndrome_model_input(bits)
        except ValueError as err:
            problems.append(f"{prefix}: {err}")
        if row["round_count"] != silver.ROUND_COUNT or row["check_count"] != silver.CHECK_COUNT:
            problems.append(f"{prefix}: shape {row['round_count']}x{row['check_count']} is not 4x4")
        if row["sample_weight"] is None or row["sample_weight"] <= 0:
            problems.append(f"{prefix}: sample_weight must be positive")
        if row["physical_fault_rate"] is not None and row["data_split"] != syndrome_data_split(
            row["physical_fault_rate"]
        ):
            problems.append(f"{prefix}: data_split does not match the course split")
        if len(problems) > 20:
            problems.append("... more problems omitted")
            break
    return problems


def export_ml_examples(connection: psycopg.Connection) -> pa.Table:
    table = fetch_ml_examples(connection)
    problems = contract_problems(table)
    if problems:
        raise ContractError("ml_syndrome_decoder_example: " + "; ".join(problems))
    return table
