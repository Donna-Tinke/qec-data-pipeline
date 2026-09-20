import pytest

from quantum_lake_student.formats import (
    b8_record_bytes,
    iter_b8_records,
    parse_01_records,
)


def test_b8_records_are_byte_aligned_and_little_endian() -> None:
    assert b8_record_bytes(9) == 2
    assert list(iter_b8_records(bytes([0b00000101]), bits_per_record=3)) == [
        (1, 0, 1)
    ]


def test_b8_rejects_partial_record() -> None:
    with pytest.raises(ValueError):
        list(iter_b8_records(b"\x00", bits_per_record=9))


def test_01_records_are_validated() -> None:
    assert parse_01_records(b"0\n1\n") == [0, 1]
    with pytest.raises(ValueError):
        parse_01_records(b"0\n2\n")
