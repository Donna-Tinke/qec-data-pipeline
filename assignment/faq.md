# Frequently asked questions

## Scope and architecture

### Is PostgreSQL itself the data lake?

No. The file-based areas hold Bronze, Silver, and ML data. PostgreSQL holds the
Gold relational model and the analytical SQL.

### Why is there an ML area after Gold?

Gold is a database model designed by your team for reliable storage and SQL.
The ML area contains two flat Parquet tables with fixed columns and data
partitions. Keeping them separate makes the hand-off from data management to
model training easy to see and test.

### Must we use exactly the illustrated architecture?

It is the recommended course design. You may change the internal organization
if you explain the reason briefly. You must still produce unchanged Bronze,
the six minimum Silver tables, a PostgreSQL Gold model, the two exact ML
tables, the required results layout, and end-to-end tracing. You cannot earn
extra marks by adding complexity.

### Which area do we design?

Gold contains the main design work. Bronze is supplied, Silver has six minimum
source-specific tables, and ML has two fixed tables. You may add helpful
Silver tables and choose how your code is organized.

### Can we build the ML tables directly from Silver?

No. All ML values must come from committed queries or views over Gold. A small
Python step may run the queries, call the supplied split helpers, validate the
result, and write Parquet.

### Does Part II need PostgreSQL running?

No. Part II reads only the two materialized ML Parquet files. PostgreSQL is
needed while Part I builds those files.

### Which sources are required?

All three: `qec_syndromes`, `google_qec`, and the curated QEC part of
`qasmbench`. There are no alternative or optional tracks.

### Must all sources join into one experiment?

No. They share QEC concepts but no reliable row-level experiment identifier.
Use clear shared names without inventing a match.

### Are we expected to copy the instructor Gold schema?

No. More than one relational design can be correct. Marks depend on clear row
meanings, suitable rules, useful queries, traceability, and justified choices.

### Does QASMBench become training data?

No. Its circuits do not have shot labels. Integrate its circuit, parity-check,
and correction information into Gold, but do not fabricate training rows.

## Cleaning and representation

### What is a Parquet file, and how do we open it?

Parquet stores a table in a compact binary form together with its column names
and data types. It is not intended to be opened in a text editor. Use the
included Pandas or PyArrow libraries to inspect its schema, row count, selected
columns, and sample rows. The step-by-step examples and project-specific uses
are in [Parquet in this assignment](plain-language-guide.md#parquet-in-this-assignment).

### May we edit source files?

No. Keep Bronze read-only and verify the supplied hashes. Correct or reinterpret
values while building Silver and record the check and decision.

### Should duplicates always be removed?

First decide what one row means. The same syndrome may validly occur with both
labels. If you remove a true duplicate, preserve evidence of its source and the
rule that identified it.

### Should we expand `quantity` into individual shots?

No. Keep aggregate syndrome rows and use `quantity` as the physical sample
weight. Expansion to 70 million rows adds no information.

### Is a measurement-one the same as a detector event?

No. Detector events are derived parity inconsistencies or changes. Keep
measurement bits, detector bits, actual logical outcomes, predictions, and
decoder mistakes separate.

### Does `actual_observable_flip = 1` mean a decoder failed?

No. A decoder fails when its prediction differs from the actual value. A
correct prediction can be zero or one.

### Must every detector bit become a Gold row?

No. A long event table and a shot table with summaries can both be reasonable.
Measure the storage effect, explain your choice, and show that your design can
rebuild the packed ML value without returning to Bronze or Silver.

## Results and tracing

### Where do data-quality issues go?

Write them to `results/part1/data_issues.parquet`. Do not mix issue records into
Silver. Each issue records its rule, severity, observed value, action, and
reason.

### Must every table repeat the full source path?

No. Each Silver row has a stable `source_record_id`. The full path, archive
member, record position, and hash live in `source_trace.parquet`. Gold and ML
use relations or views to continue the trace.

### How can one Google shot trace to several files?

Use more than one row in `source_trace.parquet` for the same logical
`source_record_id`. This records that measurements, detectors, outcomes, and
decoder predictions were aligned from separate members.

### What belongs in a run record?

At minimum: input hashes, code revision, start and end time, and per-table row
counts. Part II also records both ML input hashes, dependencies, feature order,
splits, seed, and timings.

## Implementation

### Can the main pipeline live in a notebook?

No. Notebooks are useful for discovery, but production stages and checks must
be importable and run by a documented command.

### Must we use Spark, Airflow, or Stim?

No. Python, PyArrow, and PostgreSQL are sufficient. Simulation is not required.

### How do we make repeated runs safe?

Use stable source hashes and identifiers. Replace a complete output safely or
update existing records instead of blindly appending. Prove that a second run
on unchanged input has the same identifiers and business-row counts.

### How much data should tests process?

Use tiny representative inputs for individual checks, fixed samples for
end-to-end tests, and record one successful run over the complete release.

## Part II

### Is model performance part of the grade?

**No.** You do not need to beat the supplied decoders or reach a target score.
You are graded on correct prepared-data use, meaningful baselines,
repeatability, evaluation, tests, and honest interpretation.

### How are syndrome partitions assigned?

The supplied `syndrome_data_split` helper assigns one physical fault rate to
validation, one to test, and the other five to training. Use its result.

### Why use `quantity` in metrics?

Each syndrome row represents a different number of physical observations. An
unweighted row metric answers a different question and can misstate the
logical-error rate.

### Why may decoder predictions be model inputs?

Task B is a combined decoder. It asks whether the four supplied decoders make
complementary mistakes. The helper returns event density and the four aligned
predictions.

### Why train distances three and five separately?

They represent different code distances, and their raw detector inputs contain
200 and 600 bits. Separate work keeps shapes and comparisons clear.

### Must the learned model beat the baseline?

No. A correct experiment that finds no improvement earns full credit when the
result is reported and explained honestly. Do not alter the test data to make a
model look better.

### Why include the raw-detector MLP?

It checks whether the data pipeline can serve a model that consumes packed bit
data. The required subset makes it affordable. Explain what a flat 200-bit
input does not show explicitly; no complex architecture design is required.

### Do we need to read or cite research papers?

No. This assignment has no external-reading or citation requirement. Explain
model choices and limitations from the supplied data and your implementation.

## Platform troubleshooting

### Why do the commands fail in PowerShell on Windows?

The supplied commands and Makefile require a Bash-compatible environment.
Install WSL 2 with Ubuntu if Bash is not already available, enable Docker
Desktop's integration for Ubuntu, and run the assignment in the Ubuntu
terminal. Installing Make alone does not make PowerShell compatible. Follow
the [Windows setup instructions](getting-started.md#windows-use-wsl-bash-not-powershell).

### A port is already in use.

Change the matching host port in `infrastructure/.env`, then restart. Container
connection names and ports do not change.

### `make check` reports no Bronze objects.

From `infrastructure/`, run `make data-verify` and then `make seed`.

### JupyterLab asks for a token.

The default is `quantum-course`, unless changed in `.env`.

### Docker runs out of memory.

Allocate at least 8 GB, close unused services, process data in batches, and
keep detector features packed outside the bounded prototype.

### We want to start over.

Commit source code first. `make reset-platform` deletes generated course
volumes and `make bootstrap` recreates them; it does not remove the starter
workspace or supplied release.
