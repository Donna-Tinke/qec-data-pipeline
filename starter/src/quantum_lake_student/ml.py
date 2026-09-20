"""Course-supplied representation, split, and feature helpers.

Model selection, training, evaluation, saving model results, and
interpretation remain student work.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from math import isclose
from typing import Any


MODEL_SPLITS = ("train", "validation", "test")
GOOGLE_META_PREDICTION_COLUMNS = (
    "belief_matching_prediction",
    "correlated_matching_prediction",
    "pymatching_prediction",
    "tensor_network_contraction_prediction",
)


def unpack_little_endian_bits(packed: bytes, feature_count: int) -> tuple[int, ...]:
    """Expand one Stim b8 row while discarding byte-alignment padding."""
    if feature_count <= 0:
        raise ValueError("feature_count must be positive")
    if len(packed) != (feature_count + 7) // 8:
        raise ValueError("packed byte length does not match feature_count")
    return tuple(
        (packed[index // 8] >> (index % 8)) & 1
        for index in range(feature_count)
    )


def syndrome_data_split(physical_fault_rate: float) -> str:
    """Return the course partition for one simulated fault-rate file."""
    value = float(physical_fault_rate)
    if isclose(value, 0.0005, rel_tol=0.0, abs_tol=1e-12):
        return "validation"
    if isclose(value, 0.005, rel_tol=0.0, abs_tol=1e-12):
        return "test"
    return "train"


def google_data_split(shot_index: int) -> str:
    """Return the course partition for one Google experiment shot."""
    if isinstance(shot_index, bool) or int(shot_index) != shot_index:
        raise ValueError("shot_index must be an integer")
    index = int(shot_index)
    if index < 0:
        raise ValueError("shot_index must be non-negative")
    if index % 2 == 1:
        return "test"
    if index % 10 == 8:
        return "validation"
    return "train"


def partition_records(
    records: Iterable[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    """Group prepared records by the course-supplied ``data_split`` value."""
    result: dict[str, list[Mapping[str, Any]]] = {
        split: [] for split in MODEL_SPLITS
    }
    for record in records:
        split = str(record.get("data_split", ""))
        if split not in result:
            raise ValueError(f"unexpected data_split: {split!r}")
        result[split].append(record)
    return result


def syndrome_model_input(syndrome_bits: Iterable[int | bool]) -> tuple[int, ...]:
    """Return the documented 16-value input vector for Task A."""
    bits = tuple(int(value) for value in syndrome_bits)
    if len(bits) != 16 or any(value not in (0, 1) for value in bits):
        raise ValueError("syndrome_bits must contain exactly 16 binary values")
    return bits


def google_meta_model_input(record: Mapping[str, Any]) -> tuple[float, ...]:
    """Return event density plus four decoder predictions for Task B."""
    detector_count = int(record["detector_count"])
    event_count = int(record["detector_event_count"])
    if detector_count <= 0 or not 0 <= event_count <= detector_count:
        raise ValueError("invalid detector counts")
    predictions = tuple(int(record[column]) for column in GOOGLE_META_PREDICTION_COLUMNS)
    if any(value not in (0, 1) for value in predictions):
        raise ValueError("decoder predictions must be binary")
    return (event_count / detector_count, *predictions)


def weighted_logical_error_rate(
    actual: Iterable[int | bool],
    predicted: Iterable[int | bool],
    weights: Iterable[int | float],
) -> float:
    """Return the physical-count-weighted decoder mismatch rate."""
    rows = list(zip(actual, predicted, weights, strict=True))
    total = sum(float(weight) for _, _, weight in rows)
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    errors = sum(
        float(weight)
        for observed, estimate, weight in rows
        if bool(observed) != bool(estimate)
    )
    return errors / total
