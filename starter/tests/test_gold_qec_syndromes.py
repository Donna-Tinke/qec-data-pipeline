import io
import zipfile

import psycopg
import pyarrow as pa
import pytest

from quantum_lake_student.config import Settings
from quantum_lake_student.gold import qec_syndromes as gold
from quantum_lake_student.gold.qec_syndromes import GoldLoadError
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
