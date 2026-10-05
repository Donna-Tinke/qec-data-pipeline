"""Create the required ML input tables from PostgreSQL Gold."""

from __future__ import annotations

from pathlib import Path

import psycopg
import pyarrow as pa
from psycopg import sql

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.gold import qec_syndromes
from quantum_lake_student.lake import write_parquet
from quantum_lake_student.ml import (
    MODEL_SPLITS,
    google_data_split,
    unpack_little_endian_bits,
)
from quantum_lake_student.models import StageResult
from quantum_lake_student.sources.qec_syndromes import DEFAULT_RESULTS_DIR


GOOGLE_ML_OBJECT = "ml/ml_google_decoder_example.parquet"

GOOGLE_ML_SCHEMA = pa.schema(
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


def use_gold(
    connection: psycopg.Connection,
    schema: str = qec_syndromes.GOLD_SCHEMA,
) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}").format(sql.Identifier(schema))
    )


def fetch_google_ml_examples(
    connection: psycopg.Connection,
) -> pa.Table:
    rows = connection.execute(
        """
        SELECT
            example_id,
            experiment_id,
            shot_index,
            distance,
            rounds,
            center_row,
            center_col,
            detector_count,
            detector_event_count,
            detector_bits,
            belief_matching_prediction,
            correlated_matching_prediction,
            pymatching_prediction,
            tensor_network_contraction_prediction,
            actual_observable_flip
        FROM gold.v_ml_google_decoder_example
        ORDER BY example_id
        """
    ).fetchall()

    names = GOOGLE_ML_SCHEMA.names[:-1]
    records = []

    for row in rows:
        record = dict(zip(names, row))
        record["detector_bits"] = bytes(record["detector_bits"])
        record["data_split"] = google_data_split(record["shot_index"])
        records.append(record)

    table = pa.Table.from_pylist(
        records,
        schema=GOOGLE_ML_SCHEMA,
    )

    gold_count = connection.execute(
        "SELECT count(*) FROM gold.google_shot"
    ).fetchone()[0]

    if table.num_rows != gold_count:
        raise ValueError(
            f"Google ML view has {table.num_rows} rows, "
            f"but Gold has {gold_count} shots"
        )

    return table


def validate_google_ml_examples(table: pa.Table) -> None:
    if table.schema != GOOGLE_ML_SCHEMA:
        raise ValueError("Google ML schema is incorrect")

    rows = table.to_pylist()

    example_ids = [row["example_id"] for row in rows]

    if len(example_ids) != len(set(example_ids)):
        raise ValueError("Google example_id values are not unique")

    splits = {row["data_split"] for row in rows}
    missing_splits = set(MODEL_SPLITS) - splits

    if missing_splits:
        raise ValueError(
            f"Google ML table has empty splits: {sorted(missing_splits)}"
        )

    prediction_columns = (
        "belief_matching_prediction",
        "correlated_matching_prediction",
        "pymatching_prediction",
        "tensor_network_contraction_prediction",
    )

    for row in rows:
        detector_count = row["detector_count"]
        detector_bits = row["detector_bits"]

        expected_bytes = (detector_count + 7) // 8

        if len(detector_bits) != expected_bytes:
            raise ValueError(
                f"{row['example_id']}: wrong detector_bits length"
            )

        remainder = detector_count % 8

        if remainder != 0:
            if detector_bits[-1] >> remainder != 0:
                raise ValueError(
                    f"{row['example_id']}: padding bits are not zero"
                )

        unpacked = unpack_little_endian_bits(
            detector_bits,
            detector_count,
        )

        if sum(unpacked) != row["detector_event_count"]:
            raise ValueError(
                f"{row['example_id']}: detector event count is incorrect"
            )

        if row["data_split"] != google_data_split(
            row["shot_index"]
        ):
            raise ValueError(
                f"{row['example_id']}: incorrect data split"
            )

        if row["distance"] == 3 and detector_count != 200:
            raise ValueError(
                f"{row['example_id']}: distance 3 must have 200 detectors"
            )

        if row["distance"] == 5 and detector_count != 600:
            raise ValueError(
                f"{row['example_id']}: distance 5 must have 600 detectors"
            )

        for column in prediction_columns:
            if not isinstance(row[column], bool):
                raise ValueError(
                    f"{row['example_id']}: invalid {column}"
                )

        if not isinstance(
            row["actual_observable_flip"],
            bool,
        ):
            raise ValueError(
                f"{row['example_id']}: invalid label"
            )


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path = DEFAULT_RESULTS_DIR,
) -> StageResult:
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(
        stage="ml",
        run_id=run_id,
    )

    with postgres_connection(settings) as connection:
        use_gold(connection)

        syndrome_examples = (
            qec_syndromes.export_ml_examples(connection)
        )

        google_examples = fetch_google_ml_examples(
            connection
        )
        validate_google_ml_examples(
            google_examples
        )

        qec_syndromes.run_analyses(
            connection,
            results_dir / "analysis",
        )

        qec_syndromes.write_trace_example(
            qec_syndromes.trace_example(
                connection,
                results_dir,
            ),
            results_dir,
        )

    write_parquet(
        syndrome_examples,
        qec_syndromes.ML_OBJECT,
        settings,
    )

    write_parquet(
        google_examples,
        GOOGLE_ML_OBJECT,
        settings,
    )

    result.output_count = (
        syndrome_examples.num_rows
        + google_examples.num_rows
    )

    result.finish()
    return result


if __name__ == "__main__":
    stage = run("manual")
    print(
        f"{stage.stage}: exported "
        f"{stage.output_count} total ML rows"
    )