"""Tests for the google_qec Gold model.

Pure-Python tests always run. Database tests need a PostgreSQL 16 server (the
course platform's ``postgres`` service) and are skipped when it is unreachable.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from quantum_lake_student.config import Settings
from quantum_lake_student.gold import google_qec as gold
from quantum_lake_student.sources import google_qec as silver

psycopg = pytest.importorskip("psycopg")

EXPERIMENT = "surface_code_bX_d3_r25_center_3_5"


def _experiment_row(**overrides):
    row = {
        "source_record_id": f"google_qec:{EXPERIMENT}:properties",
        "experiment_id": EXPERIMENT,
        "basis": "X",
        "distance": 3,
        "rounds": 25,
        "shots": 4,
        "center_row": 3,
        "center_col": 5,
        "measurement_count": 12,
        "detector_count": 10,
    }
    row.update(overrides)
    return row


def _shot_rows(detector_bits_per_shot):
    rows = []
    for index, bits in enumerate(detector_bits_per_shot):
        packed = silver.pack_bits(tuple(bits))
        rows.append(
            {
                "source_record_id": f"google_qec:{EXPERIMENT}:shot={index}",
                "experiment_id": EXPERIMENT,
                "shot_index": index,
                "measurement_bits": silver.pack_bits((1,) * 12),
                "sweep_bits": b"",
                "detector_bits": packed,
                "detector_event_count": sum(bits),
                "actual_observable_flip": index % 2 == 0,
                "belief_matching_prediction": True,
                "correlated_matching_prediction": False,
                "pymatching_prediction": index % 2 == 0,
                "tensor_network_contraction_prediction": False,
            }
        )
    return rows


def _silver_tables(experiments, shots) -> tuple[pa.Table, pa.Table]:
    return (
        pa.Table.from_pylist(experiments, schema=silver.EXPERIMENT_SCHEMA),
        pa.Table.from_pylist(shots, schema=silver.SHOT_SCHEMA),
    )


GOOD_BITS = [
    [1, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    [0] * 10,
    [1] * 10,
    [0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
]


TEST_SCHEMA = "gold_test"


@pytest.fixture
def connection():
    settings = Settings.from_environment()
    try:
        conn = psycopg.connect(settings.postgres_dsn, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL is not reachable")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
    conn.execute(f"SET search_path TO {TEST_SCHEMA}")
    yield conn
    conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    conn.close()


def _load(connection, experiments, shots):
    return gold.load_gold(connection, *_silver_tables(experiments, shots), schema=TEST_SCHEMA)


def test_load_builds_expected_rows_and_is_repeatable(connection):
    first = _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    ids = connection.execute(
        "SELECT array_agg(example_id ORDER BY shot_index) FROM v_google_example_lookup "
        "WHERE experiment_id = %s",
        (EXPERIMENT,),
    ).fetchone()[0]
    second = _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    ids_again = connection.execute(
        "SELECT array_agg(example_id ORDER BY shot_index) FROM v_google_example_lookup "
        "WHERE experiment_id = %s",
        (EXPERIMENT,),
    ).fetchone()[0]
    assert first == second  # no duplicate business records on a rerun
    assert first["gold.google_shot"] == 4
    assert first["gold.google_shot_prediction"] == 16  # 4 shots x 4 decoders
    assert first["gold.google_detector_position_stat"] == 10
    assert ids == ids_again and len(set(ids)) == 4


def test_ml_view_reproduces_packed_bytes_and_wide_predictions(connection):
    _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    rows = connection.execute(
        "SELECT shot_index, detector_bits, detector_event_count, detector_count, "
        "belief_matching_prediction, correlated_matching_prediction, "
        "pymatching_prediction, actual_observable_flip "
        "FROM v_ml_google_decoder_example ORDER BY shot_index"
    ).fetchall()
    assert [bytes(r[1]) for r in rows] == [silver.pack_bits(tuple(b)) for b in GOOD_BITS]
    assert [r[2] for r in rows] == [2, 0, 10, 5]
    assert all(r[3] == 10 for r in rows)
    assert all(r[4] is True and r[5] is False for r in rows)
    assert [r[6] for r in rows] == [True, False, True, False]


def test_decoder_mistake_is_derived_not_stored(connection):
    _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    mistakes = dict(
        connection.execute(
            "SELECT decoder_name, count(*) FILTER (WHERE is_logical_error) "
            "FROM v_google_decoder_outcome GROUP BY decoder_name"
        ).fetchall()
    )
    # actual flips on shots 0 and 2
    assert mistakes == {
        "belief_matching": 2,  # always True: wrong on shots 1 and 3
        "correlated_matching": 2,  # always False: wrong on shots 0 and 2
        "pymatching": 0,
        "tensor_network_contraction": 2,
    }


def test_position_stats_use_b8_bit_order(connection):
    _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    fired = dict(
        connection.execute(
            "SELECT detector_index, fired_count FROM google_detector_position_stat "
            "WHERE experiment_id = %s",
            (EXPERIMENT,),
        ).fetchall()
    )
    assert fired == {0: 2, 1: 2, 2: 1, 3: 2, 4: 1, 5: 2, 6: 1, 7: 2, 8: 2, 9: 2}


def _before_state(connection):
    return connection.execute(
        "SELECT count(*), coalesce(sum(detector_event_count), 0) FROM google_shot"
    ).fetchone()


@pytest.mark.parametrize(
    "corrupt",
    ["event_count", "padding", "length"],
)
def test_bad_silver_is_rejected_and_previous_version_survives(connection, corrupt):
    _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    before = _before_state(connection)

    shots = _shot_rows(GOOD_BITS)
    if corrupt == "event_count":
        shots[0]["detector_event_count"] += 1
    elif corrupt == "padding":  # a set padding bit (bit 12 of a 10-bit record)
        shots[1]["detector_bits"] = bytes([0, 0b00010000])
        shots[1]["detector_event_count"] = 1
    else:  # wrong number of bytes for detector_count
        shots[1]["detector_bits"] = b"\x00"
        shots[1]["detector_event_count"] = 0

    with pytest.raises((gold.GoldLoadError, psycopg.errors.CheckViolation)):
        _load(connection, [_experiment_row()], shots)
    assert _before_state(connection) == before


def test_shot_count_must_match_experiment(connection):
    with pytest.raises(gold.GoldLoadError):
        _load(connection, [_experiment_row(shots=5)], _shot_rows(GOOD_BITS))


def test_constraints_reject_invalid_rows(connection):
    _load(connection, [_experiment_row()], _shot_rows(GOOD_BITS))
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        connection.execute(
            "INSERT INTO google_shot_prediction VALUES (%s, 0, 99, true)", (EXPERIMENT,)
        )
    with pytest.raises(psycopg.errors.UniqueViolation):
        connection.execute(
            "INSERT INTO google_shot_prediction VALUES (%s, 0, 1, true)", (EXPERIMENT,)
        )
    with pytest.raises(psycopg.errors.CheckViolation):
        connection.execute("UPDATE google_experiment SET distance = 4")
