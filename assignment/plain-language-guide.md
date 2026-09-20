# Plain-language guide

This assignment uses ordinary folder names wherever possible. The terms below
may still appear in lectures, software documentation, or research papers.

| Term | Meaning in this assignment |
| --- | --- |
| Data lake | File storage that keeps the original data and the datasets produced from it. |
| Zone | One named area or stage of a data pipeline. |
| Medallion architecture | A common convention that calls source data Bronze, checked data Silver, and connected or ready-to-use data Gold. This assignment adds ML for the two training tables. |
| Grain | See *what one row represents*. `silver` keeps each source's original grain; `gold` may change it. |
| Conforming | Making two sources use the same names and types for the same idea, so they can sit in one model. This happens in `gold`, never in `silver`. |
| Schema | The column names, data types, and rules of a table or file. |
| Parquet | An open, widely used industry format for tables. It groups values by column for compression and selective reading, and preserves column names and data types. Open it with Pandas or PyArrow. See [Parquet in this assignment](plain-language-guide.md#parquet-in-this-assignment). |
| What one row represents | The real-world item described by one row, such as one experiment, shot, syndrome pattern, or decoder prediction. Some database texts call this the *grain*. |
| Source information | The source name, filename, archive member, row/member location, and hash needed to trace a result back to its origin. Most tables use a short `source_record_id` that points to these details. This is sometimes called *provenance*. |
| Source-to-result trace | A record of how an output was created from earlier data. This is sometimes called *lineage*. |
| Repeatable run | Running the same code twice on unchanged input gives the same cleaned results and does not add duplicates. This property is sometimes called *idempotence*. |
| All-or-nothing database load | Either every intended table update succeeds, or none of it is kept. Databases call this a *transaction*. |
| Folder split | Dividing a large dataset across files or folders, for example by source or run. This is often called *partitioning*. |
| ML input table | One of the two tables in the `ml` zone: documented model inputs, target, weights, and course-supplied data split. |

## The data flow in one picture

```text
bronze   supplied files (read-only)
    ↓
parsing + data checks ─────→ results/part1/ issues and run facts
    ↓
silver   cleaned Parquet, one table per source family
    ↓
gold     your PostgreSQL model + all analysis SQL
    ↓
ml       the two required training tables
    ↓
results/part2/ predictions + metrics + report
```

Bronze is read-only. Silver follows six minimum table contracts, Gold is the
relational model your team designs, and ML follows two fixed contracts. You may
motivate a different internal organization, but these compatibility outputs
and the end-to-end source trace are still required.

## Parquet in this assignment

### What is Parquet?

Apache Parquet is an open, column-oriented file format for storing tables. Like a CSV file, a Parquet file
has rows and columns. Unlike CSV, it is a compact binary format: it is intended
to be read by software rather than opened in a text editor.

**Column-oriented** means that values from the same column are stored together.
This helps compress the data and lets a reader load selected columns without
reading every field. Parquet is a widely used industry-standard format for
analytics and data lakes, with support across programming languages and tools
such as PyArrow and DuckDB. See the [Apache Parquet
overview](https://parquet.apache.org/docs/overview/).

Parquet is useful here because it:

- stores column names and data types with the data;
- supports values such as integers, Boolean values, binary data, and lists
  without first turning everything into text;
- compresses repeated values efficiently; and
- lets software read only selected columns when the whole table is not needed.

Parquet describes the **file format**, not where a file is stored. A Parquet
file can be kept on a laptop, in the supplied object store, or copied between
systems without changing its table structure.

### Where is it used in the project?

The original Bronze files stay exactly as supplied, even when they are CSV,
ZIP, QASM, `b8`, or `01` files. Your pipeline parses and checks those sources
and publishes the six required Silver tables as Parquet files.

Your team then loads its connected Gold model into PostgreSQL. Finally, it
exports two flat ML input tables from Gold as Parquet. Part II reads only those
two files. Some evidence under `results/`, including data-quality issues and
predictions, is also stored as Parquet.

```text
source files -> Silver Parquet -> Gold PostgreSQL -> ML Parquet -> models
```

Using Parquet here gives you experience with a common industry workflow and
provides compact, typed table hand-offs between pipeline stages. It does not
replace PostgreSQL, and converting a source file
to Parquet is not, by itself, sufficient cleaning.

### How to inspect a Parquet file

Pandas and PyArrow are already included in the supplied environment; students
do not need to install another Parquet program. In JupyterLab or Python, use a
small sample first:

```python
from pathlib import Path

import pyarrow.parquet as pq

path = Path("silver/qec_syndromes/syndrome_observation.parquet")
table = pq.read_table(path)

print(table.schema)
print(f"rows: {table.num_rows}")
display(table.slice(0, 5).to_pandas())
```

When only a few columns are needed, request those columns instead of loading
the full table:

```python
table = pq.read_table(
    path,
    columns=["physical_fault_rate", "logical_error_label", "quantity"],
)
```

Pandas can also read a Parquet file directly:

```python
import pandas as pd

frame = pd.read_parquet(path, columns=["physical_fault_rate", "quantity"])
```

These examples use a local path. When your implementation stores an object in
MinIO, obtain its bytes with the supplied MinIO connection helper and give
those bytes to PyArrow. The table and schema checks are otherwise the same.

### How to write a Parquet file

For a first experiment, a Pandas table can be written as follows:

```python
output = Path("silver/example.parquet")
output.parent.mkdir(parents=True, exist_ok=True)
frame.to_parquet(output, index=False, engine="pyarrow")
```

For the assignment pipeline, check the required column names and types before
publishing the file. PyArrow can apply an explicit schema and avoid storing a
Pandas index as an extra column:

```python
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("quantity", pa.int64()),
    ]
)
table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
pq.write_table(table, output, compression="zstd")
```

The example schema is deliberately small. Use the complete contracts in
[silver-tables.md](silver-tables.md) and
[required-ml-tables.md](required-ml-tables.md) for the required outputs.

### Checks to perform before publishing

For every required Parquet table, check at least:

1. what one row represents;
2. column names, data types, and missing-value rules;
3. row count and identifier uniqueness where required;
4. values and relationships required by the assignment contract;
5. whether every accepted row can be traced to its source; and
6. whether a second run safely produces the same logical output.

Do not judge a Parquet file only by whether it can be opened. A file can be
technically valid while still containing incorrect types, duplicated records,
misaligned bits, unsupported joins, or incomplete source tracing.

### Common mistakes

- Trying to read Parquet as text. Use Pandas, PyArrow, DuckDB, or another
  Parquet-aware tool.
- Allowing a library to choose unsuitable types without checking the stored
  schema.
- Accidentally writing the Pandas index as an extra column.
- Expanding aggregate data into millions of repeated rows. Keep the syndrome
  `quantity` as a weight.
- Mixing records from different sources in Silver. Source integration belongs
  in Gold.
- Appending blindly on every run. Publish a complete valid result safely so a
  rerun does not create duplicates.

If a table does not match its required schema, first compare
`table.schema` with the relevant assignment contract. The
[FAQ](faq.md) covers related questions about Silver, Gold, ML, and safe
repeated runs.
