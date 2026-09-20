from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quantum_lake_student.results import replace_source_rows

SCHEMA = pa.schema([("source_name", pa.string()), ("value", pa.string())])


def test_replace_source_rows_creates_new_file(tmp_path: Path) -> None:
    path = tmp_path / "trace.parquet"
    rows = [{"source_name": "google_qec", "value": "a"}]

    replace_source_rows(path, rows, SCHEMA, is_same_source=lambda row: row["source_name"] == "google_qec")

    assert pq.read_table(path).to_pylist() == rows


def test_replace_source_rows_keeps_other_sources_and_replaces_own(tmp_path: Path) -> None:
    path = tmp_path / "trace.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"source_name": "qasmbench", "value": "keep"},
                {"source_name": "google_qec", "value": "stale"},
            ],
            schema=SCHEMA,
        ),
        path,
    )

    replace_source_rows(
        path,
        [{"source_name": "google_qec", "value": "fresh"}],
        SCHEMA,
        is_same_source=lambda row: row["source_name"] == "google_qec",
    )

    rows = pq.read_table(path).to_pylist()
    assert {"source_name": "qasmbench", "value": "keep"} in rows
    assert {"source_name": "google_qec", "value": "fresh"} in rows
    assert {"source_name": "google_qec", "value": "stale"} not in rows
    assert len(rows) == 2
