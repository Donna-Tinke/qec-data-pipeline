import hashlib
import io
import json
import zipfile
from pathlib import Path

import psycopg
import pyarrow as pa
import pytest

from quantum_lake_student.config import Settings
from quantum_lake_student.gold import qec_syndromes as gold
from quantum_lake_student.gold.qec_syndromes import ContractError, GoldLoadError
from quantum_lake_student.sources.qec_syndromes import SYNDROME_OBSERVATION_SCHEMA, build_silver_rows

ZERO = '"((0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))"'
ONE_BIT = '"((1, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 1))"'
TWO_BIT = '"((0, 1, 0, 0), (0, 0, 0, 0), (0, 0, 1, 0), (0, 0, 0, 0))"'
TEST_SCHEMA = "gold_test"


def make_zip(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    return buffer.getvalue()


def silver_table(files: dict[str, str]) -> pa.Table:
    built = build_silver_rows(make_zip(files), input_sha256="x")
    return pa.Table.from_pylist(built.observations, schema=SYNDROME_OBSERVATION_SCHEMA)


# "nb-10" = 10 nominal shots per file, so quantities below sum to 10.
# One file per course split: 0.0005 validation, 0.005 test, 0.001 train.
SMALL_FILES = {
    "d-3_pfr-0.001000_nb-10.csv": f"labels,syndromes,quantity\n0,{ZERO},6\n0,{ONE_BIT},3\n1,{ONE_BIT},1\n",
    "d-3_pfr-0.000500_nb-10.csv": f"labels,syndromes,quantity\n0,{ZERO},9\n1,{TWO_BIT},1\n",
    "d-3_pfr-0.005000_nb-10.csv": f"labels,syndromes,quantity\n0,{ZERO},4\n1,{ONE_BIT},6\n",
}


# --- Gold load: no database needed -------------------------------------------


def test_schema_sql_is_packaged() -> None:
    assert (gold.SQL_DIR / "01_schema.sql").read_text().strip()


def test_experiment_rows_combine_silver_and_filename() -> None:
    rows = gold.experiment_rows(silver_table(SMALL_FILES))
    assert [row["physical_fault_rate"] for row in rows] == [0.0005, 0.001, 0.005]
    assert {row["distance"] for row in rows} == {3}
    assert {row["nominal_shot_count"] for row in rows} == {10}
    assert {(row["round_count"], row["check_count"]) for row in rows} == {(4, 4)}


def test_experiment_rows_reject_disagreeing_fault_rate() -> None:
    table = silver_table(SMALL_FILES)
    rates = table.column("physical_fault_rate").to_pylist()
    rates[0] = 0.25
    table = table.set_column(table.schema.get_field_index("physical_fault_rate"), "physical_fault_rate",
                             pa.array(rates, pa.float64()))
    with pytest.raises(GoldLoadError, match="disagree"):
        gold.experiment_rows(table)


def test_repeated_silver_key_stops_gold_load() -> None:
    files = {"d-3_pfr-0.001000_nb-10.csv": f"labels,syndromes,quantity\n0,{ONE_BIT},9\n0,{ONE_BIT},1\n"}
    with pytest.raises(GoldLoadError, match="repeated"):
        gold.check_unique_keys(silver_table(files))


# --- Gold load: PostgreSQL (skipped when no database is reachable) -----------


@pytest.fixture
def pg():
    settings = Settings.from_environment()
    try:
        connection = psycopg.connect(settings.postgres_dsn, connect_timeout=3, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL not reachable (start the course platform / set POSTGRES_HOST)")
    yield connection
    connection.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    connection.close()


def load_into_test_schema(connection: psycopg.Connection, table: pa.Table) -> dict[str, int]:
    return gold.load_gold(connection, table, schema=TEST_SCHEMA)


def use_test_schema(connection: psycopg.Connection) -> None:
    connection.execute(f"SET search_path TO {TEST_SCHEMA}")


def test_load_creates_normalized_tables(pg) -> None:
    counts = load_into_test_schema(pg, silver_table(SMALL_FILES))
    assert counts == {
        "simulated_experiment": 3,
        "syndrome_pattern": 3,  # ZERO, ONE_BIT, TWO_BIT stored once each
        "syndrome_observation": 7,
    }


def test_reload_leaves_other_sources_tables_alone(pg) -> None:
    # stands in for a teammate's google_* table living in the same Gold schema
    pg.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
    pg.execute(f"CREATE TABLE {TEST_SCHEMA}.google_shot (shot_index bigint)")
    pg.execute(f"INSERT INTO {TEST_SCHEMA}.google_shot VALUES (0), (1)")

    load_into_test_schema(pg, silver_table(SMALL_FILES))
    load_into_test_schema(pg, silver_table(SMALL_FILES))

    assert pg.execute(f"SELECT count(*) FROM {TEST_SCHEMA}.google_shot").fetchone()[0] == 2


def test_failure_in_another_source_rolls_back_syndromes_too(pg) -> None:
    load_into_test_schema(pg, silver_table(SMALL_FILES))
    one_file = {name: text for name, text in SMALL_FILES.items() if "0.001000" in name}

    # stages/load_postgres.run wraps all source loaders in one outer transaction
    with pytest.raises(RuntimeError, match="next source failed"):
        with pg.transaction():
            load_into_test_schema(pg, silver_table(one_file))
            raise RuntimeError("next source failed")

    remaining = pg.execute(f"SELECT count(*) FROM {TEST_SCHEMA}.syndrome_observation").fetchone()[0]
    assert remaining == 7
    assert pg.execute("SHOW search_path").fetchone()[0] != TEST_SCHEMA


def test_rerun_gives_identical_gold(pg) -> None:
    table = silver_table(SMALL_FILES)
    query = f"SELECT observation_id, syndrome_id, quantity FROM {TEST_SCHEMA}.syndrome_observation ORDER BY 1"
    first_counts = load_into_test_schema(pg, table)
    first_rows = pg.execute(query).fetchall()
    second_counts = load_into_test_schema(pg, table)
    assert second_counts == first_counts
    assert pg.execute(query).fetchall() == first_rows


def test_failed_load_keeps_previous_gold(pg) -> None:
    load_into_test_schema(pg, silver_table(SMALL_FILES))

    # a syndrome byte of 2 slips past Silver here; the Gold CHECK must abort the whole load
    rows = silver_table(SMALL_FILES).to_pylist()
    rows[-1]["syndrome_bits"] = b"\x02" + bytes(15)
    broken = pa.Table.from_pylist(rows, schema=SYNDROME_OBSERVATION_SCHEMA)
    with pytest.raises(psycopg.errors.CheckViolation):
        load_into_test_schema(pg, broken)

    remaining = pg.execute(f"SELECT count(*) FROM {TEST_SCHEMA}.syndrome_observation").fetchone()[0]
    assert remaining == 7


def test_total_below_nominal_still_loads(pg) -> None:
    # nominal 10 shots but quantities sum to 9: Silver keeps the rows with a warning,
    # Gold loads them and keeps the nominal count for comparison
    short = {"d-3_pfr-0.001000_nb-10.csv": f"labels,syndromes,quantity\n0,{ZERO},9\n"}
    load_into_test_schema(pg, silver_table(short))
    row = pg.execute(
        f"SELECT s.nominal_shot_count, sum(o.quantity) FROM {TEST_SCHEMA}.simulated_experiment s "
        f"JOIN {TEST_SCHEMA}.syndrome_observation o USING (experiment_id) GROUP BY 1"
    ).fetchone()
    assert row == (10, 9)


@pytest.mark.parametrize(
    ("statement", "error"),
    [
        ("INSERT INTO syndrome_pattern (syndrome_id, syndrome_bits) VALUES (100, '\\x00'::bytea)",
         psycopg.errors.CheckViolation),
        ("INSERT INTO syndrome_pattern (syndrome_id, syndrome_bits) "
         "VALUES (100, '\\x02000000000000000000000000000000'::bytea)",
         psycopg.errors.CheckViolation),
        ("UPDATE syndrome_observation SET quantity = 0", psycopg.errors.CheckViolation),
        ("INSERT INTO syndrome_observation SELECT experiment_id, syndrome_id, logical_error_label, 1, "
         "repeat('a', 64), 'x' FROM syndrome_observation LIMIT 1",
         psycopg.errors.UniqueViolation),
        ("UPDATE syndrome_observation SET experiment_id = 'no-such-experiment' "
         "WHERE observation_id = (SELECT min(observation_id) FROM syndrome_observation)",
         psycopg.errors.ForeignKeyViolation),
        ("UPDATE simulated_experiment SET distance = 4", psycopg.errors.CheckViolation),
    ],
)
def test_constraints_reject_bad_rows(pg, statement: str, error: type[Exception]) -> None:
    load_into_test_schema(pg, silver_table(SMALL_FILES))
    use_test_schema(pg)
    with pytest.raises(error):
        with pg.transaction():
            pg.execute(statement)


# --- Gold -> ML ----------------------------------------------------------------


def expected_example_id(experiment_id: str, bits: bytes, label: bool) -> str:
    text = f"{experiment_id}|{bits.hex()}|{'1' if label else '0'}"
    return hashlib.sha256(text.encode()).hexdigest()


def ml_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=gold.ML_SCHEMA)


def ml_row(example_id: str, fault_rate: float, split: str, bits: bytes = bytes(16)) -> dict:
    return {
        "example_id": example_id,
        "experiment_id": "e",
        "physical_fault_rate": fault_rate,
        "syndrome_bits": bits,
        "round_count": 4,
        "check_count": 4,
        "logical_error_label": False,
        "sample_weight": 1,
        "data_split": split,
    }


VALID_ROWS = [ml_row("a", 0.001, "train"), ml_row("b", 0.0005, "validation"), ml_row("c", 0.005, "test")]


def test_contract_accepts_valid_table() -> None:
    assert gold.contract_problems(ml_table(VALID_ROWS)) == []


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (lambda rows: rows.append(ml_row("a", 0.001, "train")), "not unique"),
        (lambda rows: rows.pop(2), "empty splits"),
        (lambda rows: rows[0].update(syndrome_bits=bytes(15)), "16 binary values"),
        (lambda rows: rows[0].update(syndrome_bits=b"\x02" + bytes(15)), "16 binary values"),
        (lambda rows: rows[0].update(round_count=5), "not 4x4"),
        (lambda rows: rows[0].update(sample_weight=0), "positive"),
        (lambda rows: rows[0].update(data_split="test"), "course split"),
    ],
)
def test_contract_reports_problems(change, problem: str) -> None:
    rows = [dict(row) for row in VALID_ROWS]
    change(rows)
    assert any(problem in message for message in gold.contract_problems(ml_table(rows)))


def test_ml_export_meets_contract_and_resolves_to_gold(pg) -> None:
    load_into_test_schema(pg, silver_table(SMALL_FILES))
    use_test_schema(pg)
    table = gold.export_ml_examples(pg)

    assert table.num_rows == 7
    assert set(table.column("data_split").to_pylist()) == {"train", "validation", "test"}
    for row in table.to_pylist():
        assert row["example_id"] == expected_example_id(
            row["experiment_id"], row["syndrome_bits"], row["logical_error_label"]
        )
        resolved = pg.execute(
            "SELECT source_record_id FROM syndrome_observation WHERE observation_id = %s", (row["example_id"],)
        ).fetchone()
        assert resolved is not None and resolved[0].startswith("qec_syndromes:")


def test_export_detects_rows_lost_in_view(pg, monkeypatch) -> None:
    load_into_test_schema(pg, silver_table(SMALL_FILES))
    use_test_schema(pg)
    monkeypatch.setattr(gold, "_count", lambda connection, table: 999)
    with pytest.raises(ContractError, match="999"):
        gold.export_ml_examples(pg)


# --- analysis and trace --------------------------------------------------------


def test_analysis_sql_is_packaged() -> None:
    assert sorted(path.name for path in (gold.SQL_DIR / "analysis").glob("*.sql")) == [
        "q1_syndrome_by_fault_rate.sql",
        "q1_syndrome_by_fired_count.sql",
    ]


def test_analysis_and_trace(pg, tmp_path: Path) -> None:
    load_into_test_schema(pg, silver_table(SMALL_FILES))
    use_test_schema(pg)

    counts = gold.run_analyses(pg, tmp_path / "analysis")
    assert counts["q1_syndrome_by_fault_rate"] == 3
    by_rate = (tmp_path / "analysis/q1_syndrome_by_fault_rate.csv").read_text().splitlines()
    assert by_rate[0].startswith('"physical_fault_rate"')
    # 0.0005 has no pattern with both labels: the share must be 0, not an empty (NULL) field
    for name in counts:
        for line in (tmp_path / f"analysis/{name}.csv").read_text().splitlines():
            assert ",," not in line and not line.endswith(","), f"{name}: empty value in {line!r}"

    gold.write_trace_example(gold.trace_example(pg, tmp_path), tmp_path)
    trace = json.loads((tmp_path / "trace_examples.json").read_text())["qec_syndromes"]
    # heaviest test-split example with a logical error: ONE_BIT, label 1, quantity 6
    assert trace["ml"]["data_split"] == "test"
    assert trace["ml"]["sample_weight"] == 6
    assert trace["gold"]["syndrome_observation"]["source_record_id"] == trace["silver"]["syndrome_observation"]["source_record_id"]
