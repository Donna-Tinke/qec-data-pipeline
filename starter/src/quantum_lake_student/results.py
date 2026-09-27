"""Helper for updating our own rows in a results/part1 table shared by all 3 sources.

source_trace.parquet and data_issues.parquet get written to by all three of us.
Without this, saving my rows could wipe out a teammate's rows (or duplicate my
own on a rerun). replace_source_rows only touches rows that belong to one source.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def replace_source_rows(
    path: Path,
    new_rows: list[dict[str, Any]],
    schema: pa.Schema,
    *,
    is_same_source: Callable[[dict[str, Any]], bool],
) -> None:
    existing_rows: list[dict[str, Any]] = []
    if path.exists():
        existing_rows = pq.read_table(path).to_pylist()
    kept_rows = [row for row in existing_rows if not is_same_source(row)]
    combined = kept_rows + new_rows
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(combined, schema=schema), path)
