"""Low-level helpers for the supplied QEC result formats.

These helpers deliberately handle only byte/bit representation. Deciding the
row meaning, QEC semantics, data-check decisions, and relational model remain
student work.
"""

from __future__ import annotations

from collections.abc import Iterator


def b8_record_bytes(bits_per_record: int) -> int:
    if bits_per_record <= 0:
        raise ValueError("bits_per_record must be positive")
    return (bits_per_record + 7) // 8


def iter_b8_records(data: bytes, *, bits_per_record: int) -> Iterator[tuple[int, ...]]:
    """Yield byte-aligned little-endian bit records from Stim ``b8`` data."""
    record_bytes = b8_record_bytes(bits_per_record)
    if len(data) % record_bytes:
        raise ValueError("b8 byte length is not a whole number of records")
    for offset in range(0, len(data), record_bytes):
        chunk = data[offset : offset + record_bytes]
        yield tuple(
            (chunk[bit_index // 8] >> (bit_index % 8)) & 1
            for bit_index in range(bits_per_record)
        )


def parse_01_records(data: bytes) -> list[int]:
    """Parse Stim ``01`` data containing exactly one binary value per line."""
    lines = data.splitlines()
    if any(value not in {b"0", b"1"} for value in lines):
        raise ValueError("01 data contains a value other than 0 or 1")
    return [int(value) for value in lines]
