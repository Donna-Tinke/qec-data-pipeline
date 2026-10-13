"""Tests for the Part II QEC training stage."""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from quantum_lake_student.ml import (
    google_data_split,
    google_meta_model_input,
    syndrome_data_split,
    unpack_little_endian_bits,
)
from quantum_lake_student.stages import train as train_stage


def syndrome_row(
    example_id: str,
    fault_rate: float,
    label: bool = False,
    weight: int = 10,
) -> dict:
    """Create one small valid syndrome ML row for tests."""
    return {
        "example_id": example_id,
        "experiment_id": f"experiment-{example_id}",
        "physical_fault_rate": fault_rate,
        "syndrome_bits": bytes([0, 1] * 8),
        "round_count": 4,
        "check_count": 4,
        "logical_error_label": label,
        "sample_weight": weight,
        "data_split": syndrome_data_split(fault_rate),
    }


def google_row(
    example_id: str,
    shot_index: int,
    *,
    detector_bits: bytes | None = None,
    detector_event_count: int = 0,
    label: bool = False,
) -> dict:
    """Create one small valid distance-3 Google ML row for tests."""
    if detector_bits is None:
        detector_bits = bytes(25)  # 200 bits

    return {
        "example_id": example_id,
        "experiment_id": "google-test-experiment",
        "shot_index": shot_index,
        "distance": 3,
        "rounds": 25,
        "center_row": 0,
        "center_col": 0,
        "detector_count": 200,
        "detector_event_count": detector_event_count,
        "detector_bits": detector_bits,
        "belief_matching_prediction": False,
        "correlated_matching_prediction": True,
        "pymatching_prediction": False,
        "tensor_network_contraction_prediction": True,
        "actual_observable_flip": label,
        "data_split": google_data_split(shot_index),
    }


def test_syndrome_schema_contract() -> None:
    table = pa.Table.from_pylist(
        [syndrome_row("s1", 0.001)],
        schema=pa.schema(train_stage.SYNDROME_TYPES),
    )

    train_stage._check_schema(
        table,
        train_stage.SYNDROME_TYPES,
        "syndrome",
        1,
    )


def test_schema_check_rejects_wrong_type() -> None:
    table = pa.Table.from_pylist(
        [syndrome_row("s1", 0.001)],
        schema=pa.schema(train_stage.SYNDROME_TYPES),
    )

    weight_index = table.schema.get_field_index("sample_weight")

    wrong_table = table.set_column(
        weight_index,
        "sample_weight",
        pa.array([10], type=pa.int32()),
    )

    with pytest.raises(ValueError, match="sample_weight"):
        train_stage._check_schema(
            wrong_table,
            train_stage.SYNDROME_TYPES,
            "syndrome",
            1,
        )


def test_required_split_rules() -> None:
    # Syndrome split is based on physical fault rate.
    assert syndrome_data_split(0.0005) == "validation"
    assert syndrome_data_split(0.005) == "test"
    assert syndrome_data_split(0.001) == "train"

    # Google split is based on shot_index.
    assert google_data_split(0) == "train"
    assert google_data_split(8) == "validation"
    assert google_data_split(1) == "test"
    assert google_data_split(9) == "test"
    assert google_data_split(18) == "validation"


def test_little_endian_detector_bit_order() -> None:
    # Binary 00000101 has bit 0 and bit 2 set in little-endian bit order.
    packed = bytes([0b00000101])

    bits = unpack_little_endian_bits(
        packed,
        feature_count=8,
    )

    assert bits == (
        1,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
    )

    assert sum(bits) == 2


def test_little_endian_order_across_bytes() -> None:
    # First byte has bit 0 set.
    # Second byte has its second bit set -> global detector bit 9.
    packed = bytes(
        [
            0b00000001,
            0b00000010,
        ]
    )

    bits = unpack_little_endian_bits(
        packed,
        feature_count=16,
    )

    assert bits[0] == 1
    assert bits[9] == 1
    assert sum(bits) == 2


def test_google_meta_feature_order() -> None:
    record = {
        "detector_count": 200,
        "detector_event_count": 50,
        "belief_matching_prediction": True,
        "correlated_matching_prediction": False,
        "pymatching_prediction": True,
        "tensor_network_contraction_prediction": False,
    }

    features = google_meta_model_input(record)

    assert features == (
        0.25,
        1,
        0,
        1,
        0,
    )


def test_weighted_task_a_metrics_use_sample_weight() -> None:
    actual = np.array(
        [
            0,
            1,
        ],
        dtype=np.int8,
    )

    predicted = np.array(
        [
            1,
            1,
        ],
        dtype=np.int8,
    )

    probabilities = np.array(
        [
            0.9,
            0.9,
        ],
        dtype=float,
    )

    # The first example represents nine physical observations.
    weights = np.array(
        [
            9.0,
            1.0,
        ],
    )

    metrics = train_stage.evaluate(
        actual,
        predicted,
        probabilities,
        weights,
    )

    # Wrong physical observations = 9 / 10.
    assert metrics["logical_error_rate"] == pytest.approx(0.9)

    # Weighted Brier:
    # (9 * 0.9^2 + 1 * 0.1^2) / 10 = 0.73
    assert metrics["brier_score"] == pytest.approx(0.73)

    assert metrics["balanced_accuracy"] == pytest.approx(0.5)


def test_validate_inputs_accepts_valid_small_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise validation logic without constructing all 325k real rows."""

    syndrome_rows = [
        syndrome_row("s-train", 0.001),
        syndrome_row("s-validation", 0.0005),
        syndrome_row("s-test", 0.005),
    ]

    google_rows = [
        google_row("g-train", 0),
        google_row("g-validation", 8),
        google_row("g-test", 1),
    ]

    syndrome_table = pa.Table.from_pylist(
        syndrome_rows,
        schema=pa.schema(train_stage.SYNDROME_TYPES),
    )

    google_table = pa.Table.from_pylist(
        google_rows,
        schema=pa.schema(train_stage.GOOGLE_TYPES),
    )

    # Row-count/schema behaviour is tested separately.
    # Here we want to test the deeper contract checks.
    monkeypatch.setattr(
        train_stage,
        "_check_schema",
        lambda *args, **kwargs: None,
    )

    checked_syndrome, checked_google = train_stage.validate_inputs(
        syndrome_table,
        google_table,
    )

    assert len(checked_syndrome) == 3
    assert len(checked_google) == 3


def test_validate_inputs_rejects_wrong_event_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    syndrome_rows = [
        syndrome_row("s-train", 0.001),
        syndrome_row("s-validation", 0.0005),
        syndrome_row("s-test", 0.005),
    ]

    google_rows = [
        google_row("g-train", 0),

        # detector_bits contains no events, but count incorrectly says 1.
        google_row(
            "g-validation",
            8,
            detector_bits=bytes(25),
            detector_event_count=1,
        ),

        google_row("g-test", 1),
    ]

    syndrome_table = pa.Table.from_pylist(
        syndrome_rows,
        schema=pa.schema(train_stage.SYNDROME_TYPES),
    )

    google_table = pa.Table.from_pylist(
        google_rows,
        schema=pa.schema(train_stage.GOOGLE_TYPES),
    )

    monkeypatch.setattr(
        train_stage,
        "_check_schema",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(
        ValueError,
        match="detector_event_count mismatch",
    ):
        train_stage.validate_inputs(
            syndrome_table,
            google_table,
        )


def test_threshold_choice_is_repeatable() -> None:
    labels = np.array(
        [
            0,
            0,
            1,
            1,
        ],
        dtype=np.int8,
    )

    probabilities = np.array(
        [
            0.10,
            0.40,
            0.60,
            0.90,
        ],
    )

    first = train_stage.choose_threshold(
        labels,
        probabilities,
    )

    second = train_stage.choose_threshold(
        labels,
        probabilities,
    )

    assert first == second
    assert 0.10 <= first <= 0.90


def test_fixed_seed_logistic_model_is_repeatable() -> None:
    x = np.array(
        [
            [0.0, 0.0],
            [0.0, 1.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.1, 0.2],
            [0.8, 0.9],
        ],
    )

    y = np.array(
        [
            0,
            0,
            0,
            1,
            0,
            1,
        ],
    )

    first = train_stage.LogisticRegression(
        max_iter=200,
        random_state=train_stage.SEED,
    )

    second = train_stage.LogisticRegression(
        max_iter=200,
        random_state=train_stage.SEED,
    )

    first.fit(x, y)
    second.fit(x, y)

    np.testing.assert_allclose(
        first.coef_,
        second.coef_,
    )

    np.testing.assert_allclose(
        first.predict_proba(x),
        second.predict_proba(x),
    )


def test_prediction_schema_allows_missing_probability() -> None:
    """Supplied decoders have no probability, so Brier is N/A."""

    table = pa.Table.from_pylist(
        [
            {
                "example_id": "example-1",
                "model_id": "belief_matching",
                "label": False,
                "prediction": True,
                "probability": None,
                "split": "test",
            }
        ],
        schema=train_stage.PRED_SCHEMA,
    )

    assert table.num_rows == 1
    assert table["probability"].null_count == 1