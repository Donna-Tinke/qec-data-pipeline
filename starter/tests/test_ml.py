from quantum_lake_student.ml import (
    google_data_split,
    google_meta_model_input,
    partition_records,
    syndrome_data_split,
    syndrome_model_input,
    unpack_little_endian_bits,
    weighted_logical_error_rate,
)


def test_unpack_little_endian_bits_discards_padding():
    assert unpack_little_endian_bits(bytes([0b10000101, 0b00000011]), 10) == (
        1, 0, 1, 0, 0, 0, 0, 1, 1, 1
    )


def test_course_split_helpers_return_documented_partitions():
    assert syndrome_data_split(0.0005) == "validation"
    assert syndrome_data_split(0.005) == "test"
    assert syndrome_data_split(0.001) == "train"
    assert google_data_split(1) == "test"
    assert google_data_split(8) == "validation"
    assert google_data_split(10) == "train"


def test_partition_records_uses_prepared_split_column():
    result = partition_records(
        [{"data_split": "train", "id": 1}, {"data_split": "test", "id": 2}]
    )
    assert [record["id"] for record in result["train"]] == [1]
    assert result["validation"] == []
    assert [record["id"] for record in result["test"]] == [2]


def test_model_input_helpers_return_only_documented_values():
    assert syndrome_model_input([0, 1] * 8) == tuple([0, 1] * 8)
    record = {
        "detector_count": 200,
        "detector_event_count": 50,
        "belief_matching_prediction": 0,
        "correlated_matching_prediction": 1,
        "pymatching_prediction": 0,
        "tensor_network_contraction_prediction": 1,
        "actual_observable_flip": 1,
    }
    assert google_meta_model_input(record) == (0.25, 0, 1, 0, 1)


def test_weighted_logical_error_rate_uses_physical_counts():
    assert weighted_logical_error_rate([0, 1], [0, 0], [9, 1]) == 0.1
