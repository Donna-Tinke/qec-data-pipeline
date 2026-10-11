# Part I Design Report — `qec_syndromes` 

---

## 1. Source Discovery and Row Meanings

### 1.1 `qec_syndromes` (`bronze/source=qec_syndromes/syndromes_dataset.zip`)

The archive contains 8 members: one `README.txt` and seven CSV files, one per physical fault rate. Every filename follows `d-<distance>_pfr-<physical fault rate>_nb-<nominal samples>.csv`, so distance, fault rate, and nominal shot count are encoded in the name rather than in the data:

| Archive member | Fault rate | CSV rows | Distinct syndromes | Shots with label 1 | ML split |
| --- | ---: | ---: | ---: | ---: | --- |
| `d-3_pfr-0.000010_nb-10M.csv` | 0.00001 | 68 | 68 | 2,340 | train |
| `d-3_pfr-0.000050_nb-10M.csv` | 0.00005 | 215 | 202 | 11,542 | train |
| `d-3_pfr-0.000100_nb-10M.csv` | 0.0001 | 491 | 445 | 22,910 | train |
| `d-3_pfr-0.000500_nb-10M.csv` | 0.0005 | 1,407 | 1,228 | 114,403 | validation |
| `d-3_pfr-0.001000_nb-10M.csv` | 0.001 | 2,854 | 2,387 | 227,018 | train |
| `d-3_pfr-0.005000_nb-10M.csv` | 0.005 | 20,887 | 14,643 | 1,039,500 | test |
| `d-3_pfr-0.010000_nb-10M.csv` | 0.01 | 49,676 | 31,466 | 1,865,274 | train |
| **Total** | | **75,598** | **31,941** (across files) | **3,282,987** | |

**File format.** Each CSV has a header and three columns:

```text
labels,syndromes,quantity
0,"((0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))",9987291
0,"((0, 0, 1, 0), (0, 0, 1, 0), (0, 0, 0, 0), (0, 0, 0, 0))",486
1,"((0, 0, 1, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))",476
```

- `labels` is the simulated logical-error outcome (0/1).
- `syndromes` is a **string** holding a nested Python tuple of 4 rounds × 4 binary check values. It is a packed/nested value and has to be parsed.
- `quantity` is the number of simulated shots that produced this exact (syndrome, label) combination. It is an **aggregate weight**: the seven files sum to exactly 70,000,000 shots (10M each), stored in 75,598 rows.

**Differences between documentation and data.** We found two:
1. `README.txt` documents a column called `label`, but every CSV header says `labels`.
2. The README describes the syndrome as "tuples of the form (round, syndromes)". The data actually holds a 4-tuple of rounds, each a 4-tuple of check values, with no round index stored.

**What one row represents.** One CSV row is one *(experiment, syndrome sequence, logical label)* aggregate, i.e. all shots in one fault-rate simulation that produced this syndrome and this label.

**Candidate identifiers.**
- *Syndrome alone*: **not unique within a file.** 25,159 of the 50,439 (experiment, syndrome) pairs appear twice, once with label 0 and once with label 1 (see §4.2).
- *(experiment, syndrome, label)*: **unique.** All 75,598 rows are distinct on this key (0 repeated keys), so it is the natural key. The experiment is the filename stem (e.g. `d-3_pfr-0.005000_nb-10M`).
- *Source position*: `(archive member, CSV line number)` identifies the record in Bronze. It becomes `source_record_id = "qec_syndromes:<member>:line=<n>"`.

**Relationships.** All rows in a file share one experiment (1:N). The same syndrome pattern recurs across experiments: 14,187 of the 31,941 distinct patterns occur in more than one fault-rate file, and 57 occur in all seven. This is the basis for the separate `syndrome_pattern` Gold table (§3.1).

**Companion files.** There are none. Each CSV is self-contained, and `README.txt` is documentation that the pipeline does not need. The required content is the seven CSV files themselves (§5.1).

---

## 2. Architecture, Silver Layer, and Reproduction Commands

The four-stage architecture and the reproduction commands are in §2.1 and §2.3 of the integration draft. The syndrome source plugs into it at three points:

| Stage | Entry point | Output |
| --- | --- | --- |
| Silver | [`sources/qec_syndromes.py`](../src/quantum_lake_student/sources/qec_syndromes.py) `run()` (called by [`stages/prepare_data.py`](../src/quantum_lake_student/stages/prepare_data.py)) | `silver/qec_syndromes/syndrome_observation.parquet`, our rows in `source_trace.parquet` / `data_issues.parquet`, `results/part1/checks/qec_syndromes.json` |
| Gold | [`gold/qec_syndromes.py`](../src/quantum_lake_student/gold/qec_syndromes.py) `run()` → `load_gold()` (called inside the single transaction of [`stages/load_postgres.py`](../src/quantum_lake_student/stages/load_postgres.py)) | `gold.simulated_experiment`, `gold.syndrome_pattern`, `gold.syndrome_observation`, view `v_ml_syndrome_decoder_example` |
| ML + analysis | `export_ml_examples()` and `run_analyses()` (called by [`stages/build_ml_tables.py`](../src/quantum_lake_student/stages/build_ml_tables.py)); `trace_example()` (called by [`runner.py`](../src/quantum_lake_student/runner.py)) | `ml/ml_syndrome_decoder_example.parquet`, `results/part1/analysis/q1_*.csv`, `qec_syndromes` entry in `trace_examples.json` |

Syndrome-only tests: `python -m pytest tests/test_qec_syndromes.py tests/test_gold_qec_syndromes.py` (19 Silver + 17 Gold/ML test functions. The PostgreSQL tests skip when no database is reachable and use a throwaway `gold_test` schema otherwise).

### 2.4 `qec_syndromes` Silver Table Construction ([`sources/qec_syndromes.py`](../src/quantum_lake_student/sources/qec_syndromes.py))

`build_silver_rows()` processes the archive **in memory** in four steps:

1. **Archive read and hash.** Read the Bronze ZIP bytes from MinIO and compute their SHA-256 (`input_sha256 = bdfce36a…`), which is stored on every trace row. Manifest verification is done earlier by `register_sources`.
2. **Member safety and inventory.** `check_member_name()` rejects absolute paths, leading `/` or `\`, and any `..` component (zip-slip) **before** a member is read, and **stops the run**. Only `.csv` members are processed (`README.txt` is skipped). An archive with no CSVs at all stops the run, and a CSV count other than 7 is recorded.
3. **File-level checks** (`process_csv()`), in this order:
   - parse the filename with a strict regex (`d-(\d+)_pfr-(\d+(\.\d+)?)_nb-(\d+)([KM]?)\.csv`), and expand `nb-10M` to 10,000,000;
   - require distance 3 (the only distance with a 4×4 syndrome);
   - require the header to be `labels,syndromes,quantity` (actual) or `label,syndromes,quantity` (documented).

   A file that fails the filename, distance, or unknown-header check is rejected as a whole, and every one of its lines is counted as rejected.
4. **Row-level checks and conversion.** For every data line:
   - a source-trace row is written first, so **rejected rows are traceable too**;
   - the line must have exactly 3 CSV fields;
   - `labels` must be `0`/`1`, and becomes a `bool`;
   - `syndromes` is parsed with `ast.literal_eval`, which accepts literals only, so no code in the CSV can execute;
   - the shape must be exactly 4 rounds × 4 checks, and every value an `int` in {0, 1} (`True`/`False` are excluded explicitly);
   - the 16 values are flattened **round-major** (all four checks of round 0, then round 1, …) into a 16-byte `binary` value of `0x00`/`0x01` bytes;
   - `quantity` must be a positive integer, and is kept as a weight, **never expanded** into individual shots.

   A row that fails any rule is excluded from Silver and written to `data_issues.parquet` with its original line and reason. After all rows, the accepted weights are summed and compared with the filename's nominal count.

Shared evidence files are updated with `results.replace_source_rows()`, which replaces only rows whose `source_name` (trace) or `rule_id` prefix (issues) belongs to `qec_syndromes`. `issue_id` hashes `(rule_id, locator)` and not the run id, so a rerun on unchanged input produces identical issue rows.

#### `silver/qec_syndromes/syndrome_observation.parquet` (75,598 rows)
| Column | Arrow Type | Derivation Rule | Example (trace row, §6) |
| --- | --- | --- | --- |
| `source_record_id` | `string` | `"qec_syndromes:<archive_member>:line=<csv line>"` (header is line 1) | `"qec_syndromes:d-3_pfr-0.005000_nb-10M.csv:line=3"` |
| `experiment_id` | `string` | Filename stem | `"d-3_pfr-0.005000_nb-10M"` |
| `physical_fault_rate` | `float64` | `pfr-` part of the filename | `0.005` |
| `syndrome_bits` | `binary` | 16 one-byte 0/1 values, round first then check | `0x00000100000000000000000000000000` (round 0, check 2 fired) |
| `round_count` | `int32` | Constant 4, enforced by the shape check | `4` |
| `check_count` | `int32` | Constant 4, enforced by the shape check | `4` |
| `logical_error_label` | `bool` | `labels == "1"` | `true` |
| `quantity` | `int64` | `quantity`, > 0 | `141407` |

---

## 3. Gold Tables, Relationships, Keys, Constraints, and Indexes

### 3.1 `qec_syndromes` Gold Tables ([`gold/sql/qec_syndromes/01_schema.sql`](../src/quantum_lake_student/gold/sql/qec_syndromes/01_schema.sql))

Silver repeats the 16 syndrome bytes on every row, and repeats the experiment's fault rate and shape 75,598 times. Gold separates the three things a row mixes together: the **experiment** (one per file), the **syndrome pattern** (one per distinct 16-bit value), and the **observation** (the weighted (experiment, pattern, label) fact that links them).

```text
simulated_experiment 1 ──< syndrome_observation >── 1 syndrome_pattern
   (7 rows)                    (75,598 rows)              (31,941 rows)
```

| Gold Table | One row represents... | Primary Key | Foreign Keys, Constraints & Indexes | Rows |
| --- | --- | --- | --- | ---: |
| `simulated_experiment` | One simulated d=3 fault-rate experiment (one source CSV file). | `(experiment_id)` | `UNIQUE (physical_fault_rate)`, `CHECK (physical_fault_rate > 0 AND < 1)`, `CHECK (distance >= 3 AND distance % 2 = 1)`, `CHECK (round_count > 0)`, `CHECK (check_count > 0)`, `CHECK (nominal_shot_count > 0)` | 7 |
| `syndrome_pattern` | One distinct 16-value syndrome (4 rounds × 4 checks), shared by every experiment and label it occurs with. | `(syndrome_id)` | `UNIQUE (syndrome_bits)`, `CHECK (length(syndrome_bits) = 16)`, `CHECK (btrim(syndrome_bits, '\x0001') = '')` (every byte is 0 or 1), `fired_count` = `GENERATED ALWAYS AS (bit_count(syndrome_bits)) STORED` | 31,941 |
| `syndrome_observation` | One aggregate (experiment, syndrome, label) source CSV row; `quantity` = number of shots it represents. | `(experiment_id, syndrome_id, logical_error_label)` | `FK (experiment_id) → simulated_experiment`, `FK (syndrome_id) → syndrome_pattern`, `CHECK (quantity > 0)`, `UNIQUE (observation_id)` + `CHECK (observation_id ~ '^[0-9a-f]{64}$')`, `UNIQUE (source_record_id)`, `INDEX (syndrome_id)` | 75,598 |

Supporting objects in the same file:
- **`syndrome_observation_id(experiment_id, syndrome_bits, label)`** is an `IMMUTABLE` SQL function returning `sha256(experiment_id | hex(syndrome_bits) | label)`. It is the single definition of the ML `example_id`, and is stored as `syndrome_observation.observation_id`.
- **`v_ml_syndrome_decoder_example`** is the Gold → ML view. It joins all three tables and returns every ML column except `data_split`, which the export adds with the course helper `syndrome_data_split()`. One view row = one ML example, and `example_id = observation_id`.

---

## 4. Design Decisions, Storage Trade-offs, and Rejected Relationship

### 4.1 `qec_syndromes` Gold Design Decisions

- **Separate `syndrome_pattern` table instead of copying Silver.** "Syndrome patterns and values" are a required Gold concept, and patterns recur across fault rates (§1.1). Storing each pattern once has three benefits:
  - pattern-level attributes such as `fired_count` are computed once, by PostgreSQL, as a generated column;
  - "the same pattern across fault rates or labels" becomes a plain join on `syndrome_id`, supported by `INDEX (syndrome_id)`;
  - the `CHECK` constraints on the 16 bytes are enforced on 31,941 rows instead of 75,598.

  The storage saving is real but small (16 B × 75,598 ≈ 1.21 MB of syndrome bytes in Silver, versus 16 B × 31,941 ≈ 0.51 MB plus a 4-byte FK per observation ≈ 0.30 MB). It was not the main reason for the split. The view reassembles the exact Silver bytes, so the ML export never re-reads Silver or Bronze.
- **Key = `(experiment_id, syndrome_id, logical_error_label)`.** The label must be part of the key because one syndrome legitimately occurs with both labels (§4.2). We keep the two label rows apart instead of collapsing them into "count of label 0 / count of label 1" columns. That way one Gold row still corresponds to exactly one CSV row and one `source_record_id`, which keeps tracing 1:1.
- **Stable identifiers that do not depend on load order.**
  - `syndrome_id` is assigned with `row_number() OVER (ORDER BY syndrome_bits)`, so reloading unchanged input gives identical ids (tested in `test_rerun_gives_identical_gold`).
  - The ML `example_id` deliberately does **not** use `syndrome_id`. It hashes the natural key (`experiment_id`, the raw syndrome bytes, the label), so an example keeps its id even if a new pattern were added and shifted the surrogate numbering.
  - The hash is defined once, as a SQL function, and the tests recompute it independently in Python.
- **`quantity` stays a weight.** Expanding it would create 70M rows with no new information (FAQ). Every Q1 aggregate is weighted with `sum(quantity)`, and §7.1 shows how large the error would be otherwise.
- **`nominal_shot_count` stored on the experiment.** The `nb-10M` filename value is kept so that SQL can compare it with `sum(quantity)`. A mismatch is a Silver *warning*, not a Gold failure (`test_total_below_nominal_still_loads`). For this release the two are equal for all 7 files.
- **`UNIQUE (physical_fault_rate)`.** The course split is defined by fault rate, so in this single-distance release, fault rate must identify exactly one experiment. This is a deliberate restriction: a future file with another distance at the same rate would need the key widened to `(distance, physical_fault_rate)`.
- **`data_split` is not stored in Gold.** It is computed at export time by `syndrome_data_split()`, so the split rule exists in one place only (the course helper) and cannot drift from a stored copy.
- **All-or-nothing load.** `load_gold()` does the following inside **one transaction**:
  - drops and recreates only its own objects;
  - bulk-loads Silver through a `COPY` into a temp table;
  - inserts patterns, then observations;
  - runs Silver ↔ Gold reconciliation: row counts per table, plus `sum(quantity)` per experiment compared with Silver's accepted weight.

  A failed check or constraint raises, and the whole transaction rolls back.

  When run from `stages/load_postgres.py`, this block becomes a savepoint inside the single outer transaction that also loads Google and QASMBench. A failure in **any** source therefore rolls back **all** of them (`test_failure_in_another_source_rolls_back_syndromes_too`, `test_failed_load_keeps_previous_gold`). Before touching the database, `check_unique_keys()` turns a repeated Silver key into a readable `GoldLoadError` instead of a bare primary-key violation.

### 4.2 Investigated and Rejected Relationships (with Evidence)

**(a) Rejected: linking `simulated_experiment` to the Google distance-3 experiments.** Both sources are "distance-3 surface code" data with logical-error labels, so a join on `distance = 3` (or a shared experiment table) looked plausible. We compared the two sources directly (Google values from each `properties.yml`):

| Property | `qec_syndromes` | `google_qec` (d=3) |
| --- | --- | --- |
| Origin | Simulation, swept physical fault rate | Hardware (4 processor locations), no fault-rate field |
| Rounds | 4 | 25 |
| Values per shot | 16 syndrome values (4 checks × 4 rounds) | 200 detector bits (8 measure qubits, 17 qubits total) |
| Granularity | Aggregate rows with `quantity` weight | One row per shot (50,000 per experiment) |
| Shared identifier | — | none (no common id, layout, basis field, or seed) |

The only shared value is `distance = 3`. That is vocabulary, not an identifier: every simulated experiment would match all four Google d=3 experiments (a 7 × 4 many-to-many), and the joined rows would describe different circuits with incompatible syndrome shapes. Following the brief ("shared vocabulary does not create a shared identifier"), the two sources stay separate entities. The Google `decoder`/prediction tables are not reused for simulated data either, because the syndrome source supplies no decoder output.

**(b) Rejected: syndrome → label as a functional dependency** (i.e. keying observations by `(experiment, syndrome)`, or storing the label on `syndrome_pattern`). In the data:
- 25,159 of the 50,439 (experiment, syndrome) pairs occur with **both** labels, and 18,320 distinct patterns have both labels in at least one experiment;
- even the all-zero syndrome carries label 1 in four files: 2, 2, 210, and 1,010 shots at fault rates 0.0005, 0.001, 0.005, and 0.01.

The syndrome therefore does not determine the label. The label is part of the observation key, and `syndrome_pattern` has no label column. The trace example in §6 is itself such a case: its pattern appears in the test file with label 1 (line 3, 141,407 shots) and with label 0 (line 100, 2,593 shots).

---

## 5. Data Quality Findings and Count Reconciliation

### 5.1 `qec_syndromes` Checks and Findings

**Structural failures stop the run**, raised before any Silver output is written: an unsafe archive member path (`qec_syndromes.unsafe_archive_member`) or an archive with no CSV files. Archive presence, size, and SHA-256 are checked earlier, against the manifest, by `register_sources`.

**Record-level and file-level rules** go to `results/part1/data_issues.parquet`. The outcome of *every* check, including the ones that pass, is written to `results/part1/checks/qec_syndromes.json` (per-file row counts, weighted totals, label weights, distinct syndromes, syndromes with both labels).

| Rule (`rule_id` suffix) | Level | Severity → action | What it checks | Outcome on this release |
| --- | --- | --- | --- | --- |
| `header_documented_label` | file | WARNING → kept | Header is `labels` while the README says `label` | **7 issues** (every file); actual header used |
| `header` | file | ERROR → file rejected | Header is neither the actual nor the documented form | 0 |
| `filename_pattern` | file | ERROR → file rejected | Name matches `d-…_pfr-…_nb-….csv` | 0 |
| `distance` | file | ERROR → file rejected | Distance is 3 (4×4 syndrome) | 0 |
| `csv_file_count` | archive | WARNING → kept | Exactly 7 fault-rate CSVs | 0 (7 found) |
| `weighted_total` | file | WARNING → kept | Accepted `sum(quantity)` equals the filename's nominal count | 0 (all 7 files sum to exactly 10,000,000) |
| `column_count` | row | ERROR → rejected | Exactly 3 fields | 0 |
| `label_domain` | row | ERROR → rejected | `labels` ∈ {0, 1} | 0 |
| `syndrome_parse` | row | ERROR → rejected | `syndromes` is a tuple literal | 0 |
| `syndrome_shape` | row | ERROR → rejected | 4 rounds × 4 checks | 0 |
| `syndrome_domain` | row | ERROR → rejected | Every value is the integer 0 or 1 | 0 |
| `quantity` | row | ERROR → rejected | Positive integer | 0 |
| `duplicate_syndrome_label` | row | WARNING → kept | (syndrome, label) repeated within a file | 0 |

The supplied data is clean at row level: **0 of 75,598 rows rejected**, and the only findings are the 7 header warnings. Every rule has a test that feeds it a deliberately broken row or file ([`tests/test_qec_syndromes.py`](../tests/test_qec_syndromes.py)), including the **valid** case where one syndrome occurs with both labels (`test_same_syndrome_with_both_labels_is_valid`), which must *not* raise an issue. In this release that case is common (25,159 pairs, §4.2), so the pipeline does not count it as an error.

**Gold adds a second line of defence.** If Silver ever let a bad value through, the database constraints reject it, and the whole load rolls back. Examples: a non-0/1 byte, a short pattern, `quantity = 0`, an unknown experiment, or an even distance (all exercised in `test_constraints_reject_bad_rows`).

### 5.2 Bronze → Silver → Gold → ML Reconciliation (`qec_syndromes` rows)

| Layer | Object | Rows | Weighted shots | Check |
| --- | --- | ---: | ---: | --- |
| Bronze | 7 CSV members (data lines) | 75,598 | 70,000,000 | `rows_read = accepted + rejected` (`rows_reconcile: true`) |
| Silver | `syndrome_observation.parquet` | 75,598 accepted / 0 rejected | 70,000,000 | `weighted_total` equals 7 × 10M nominal |
| Gold | `simulated_experiment` / `syndrome_pattern` / `syndrome_observation` | 7 / 31,941 / 75,598 | 70,000,000 | `check_load()`: counts equal (experiments, distinct Silver patterns, Silver rows) and `sum(quantity)` per experiment equals Silver; otherwise rollback |
| ML | `ml_syndrome_decoder_example.parquet` | 75,598 (train 53,304 / validation 1,407 / test 20,887) | 70,000,000 | `fetch_ml_examples()` requires view rows = Gold observations; `contract_problems()` validates schema, unique ids, no nulls, 16 binary values, 4×4 shape, positive weights, and course split |

---

## 6. End-to-End Source Trace — Syndrome Example ([`results/part1/trace_examples.json`](../results/part1/trace_examples.json), key `qec_syndromes`)

`trace_example()` picks the example by a fixed rule: the **heaviest test-split observation with a logical error** (`physical_fault_rate = 0.005`, label true, ordered by `quantity DESC, observation_id`). The choice is therefore repeatable, and the example always has Part II test predictions. Each hop uses a key stored in the layer below:

| Hop | Lookup | Values |
| --- | --- | --- |
| **Part II prediction** | `results/part2/predictions.parquet` where `example_id = …` | `model_id = task_a_weighted_logistic`, `split = test`, `label = true`, `prediction = true`, `probability = 0.1487` (prior baseline `task_a_weighted_prior`: `prediction = false`, `probability = 0.0426`) |
| **ML** | `ml/ml_syndrome_decoder_example.parquet` | `example_id = a47305529fb5dded09d25a1c5d21d1da44d3b7ca6ca24673918beec3fd6317b1`, `data_split = test`, `logical_error_label = true`, `sample_weight = 141407` |
| **Gold** | `syndrome_observation.observation_id = example_id` (= `syndrome_observation_id('d-3_pfr-0.005000_nb-10M', bits, true)`) | `experiment_id = d-3_pfr-0.005000_nb-10M` → `simulated_experiment` (`physical_fault_rate = 0.005`, `nominal_shot_count = 10000000`); `syndrome_id = 6468` → `syndrome_pattern` (`syndrome_bits = 00000100000000000000000000000000`, `fired_count = 1`); `quantity = 141407`; `source_record_id = qec_syndromes:d-3_pfr-0.005000_nb-10M.csv:line=3` |
| **Silver** | `silver/qec_syndromes/syndrome_observation.parquet` where `source_record_id = …` | same `source_record_id`, identical bits / label / quantity |
| **Bronze** | `results/part1/source_trace.parquet` where `source_record_id = …` | `bronze_object = bronze/source=qec_syndromes/syndromes_dataset.zip`, `archive_member = d-3_pfr-0.005000_nb-10M.csv`, `record_locator = line=3`, `input_sha256 = bdfce36a71f04295ac78fb372d9c2e381801c05e3be119f919750ef59026d072` |

Line 3 of that CSV is `1,"((0, 0, 1, 0), (0, 0, 0, 0), (0, 0, 0, 0), (0, 0, 0, 0))",141407`: round 0, check 2 is the single fired check, which is byte 2 of the round-major encoding. The trace JSON is generated during Part I. The prediction hop is checked after Part II by matching the same `example_id` in `predictions.parquet`.

---

## 7. Part I SQL Analyses and Interpretations

### 7.1 Question 1 — How do weighted syndrome frequency and logical-error labels change with physical fault rate?
- **SQL** (both join `syndrome_observation`, `simulated_experiment`, and `syndrome_pattern`, i.e. 3 Gold tables): [`gold/sql/qec_syndromes/analysis/q1_syndrome_by_fault_rate.sql`](../src/quantum_lake_student/gold/sql/qec_syndromes/analysis/q1_syndrome_by_fault_rate.sql) (one row per fault rate) and [`q1_syndrome_by_fired_count.sql`](../src/quantum_lake_student/gold/sql/qec_syndromes/analysis/q1_syndrome_by_fired_count.sql) (one row per fault rate × number of fired checks)
- **Outputs**: [`results/part1/analysis/q1_syndrome_by_fault_rate.csv`](../results/part1/analysis/q1_syndrome_by_fault_rate.csv) and [`q1_syndrome_by_fired_count.csv`](../results/part1/analysis/q1_syndrome_by_fired_count.csv)

All rates below are weighted by `quantity` (shots), not by rows.

**Q1a — per fault rate** (rounded; full precision in the CSV):

| Fault rate | Agg. rows | Distinct patterns | Shots | Weighted logical-error rate | All-zero syndrome share | Mean fired checks | Shots in both-label patterns |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.00001 | 68 | 68 | 10M | 0.023 % | 99.87 % | 0.002 | 0.0 % |
| 0.00005 | 215 | 202 | 10M | 0.115 % | 99.36 % | 0.010 | 0.1 % |
| 0.0001 | 491 | 445 | 10M | 0.229 % | 98.74 % | 0.021 | 0.5 % |
| 0.0005 | 1,407 | 1,228 | 10M | 1.144 % | 93.84 % | 0.103 | 97.6 % |
| 0.001 | 2,854 | 2,387 | 10M | 2.270 % | 88.06 % | 0.205 | 95.9 % |
| 0.005 | 20,887 | 14,643 | 10M | 10.395 % | 53.10 % | 0.973 | 99.6 % |
| 0.01 | 49,676 | 31,466 | 10M | 18.653 % | 28.40 % | 1.825 | 99.7 % |

**Q1b — weighted logical-error rate by number of fired checks** (excerpt; shots in brackets):

| Fired checks | p = 0.0001 | p = 0.001 | p = 0.005 | p = 0.01 |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 0.000 (9.87M) | 0.000 (8.81M) | 0.000 (5.31M) | 0.0004 (2.84M) |
| 1 | 0.499 (45.5k) | 0.501 (413k) | 0.500 (1.31M) | 0.500 (1.48M) |
| 2 | 0.001 (79.8k) | 0.007 (727k) | 0.033 (2.39M) | 0.067 (2.86M) |
| 3 | 0.458 (336) | 0.494 (29.3k) | 0.500 (485k) | 0.499 (1.17M) |
| 4 | 0.000 (237) | 0.013 (22.6k) | 0.062 (404k) | 0.117 (1.06M) |
| 5 | 0.000 (1) | 0.506 (815) | 0.501 (66.9k) | 0.500 (338k) |
| 6 | — | 0.008 (377) | 0.084 (33.4k) | 0.153 (187k) |

**Interpretation** (associations in this simulated data; no causal claims):
- **Logical-error labels rise steadily with fault rate.** The weighted rate grows from 0.023 % at p = 0.00001 to 18.7 % at p = 0.01. Up to p = 0.001 it stays close to a constant multiple of the fault rate (≈ 23 × p). Above that it grows more slowly (≈ 21 × p at 0.005 and ≈ 19 × p at 0.01).
- **Syndromes shift from almost always trivial to mostly non-trivial.** The share of shots with an all-zero syndrome falls from 99.9 % to 28.4 %, and the mean number of fired checks rises from 0.002 to 1.8. The variety of observed patterns grows from 68 to 31,466, which is why the 0.01 file holds two-thirds of all aggregate rows even though every file represents the same 10M shots.
- **Row counts must not be used as frequencies.** At p = 0.00001, 10 of 68 rows (14.7 %) have label 1, but they represent only 0.023 % of shots, an overstatement of roughly 600×. An unweighted row rate measures how many *distinct* (pattern, label) combinations exist, not how often errors happen. This is why the analyses, the ML `sample_weight`, and Part II Task A all weight by `quantity`.
- **The number of fired checks is strongly associated with the label.** At every fault rate, patterns with an **odd** number of fired checks have a logical-error rate of ≈ 0.50, and they account for 81–100 % of all label-1 shots. Patterns with an **even**, non-zero count have much lower rates, which grow with the fault rate (e.g. 2 fired checks: 0.1 % at p = 0.0001 → 6.7 % at p = 0.01). The data does not explain *why*. One reading consistent with it is that an odd-count syndrome is ambiguous between corrections that differ by a logical operator, but we do not claim that mechanism from this table. Cells with very few shots (e.g. 3 shots with 3 fired checks at p = 0.00001) are too small to interpret.
- **The same syndrome increasingly occurs with both labels.** From p = 0.0005 upward, over 95 % of shots fall in (experiment, pattern) pairs that appear with both labels. This is driven mainly by the all-zero pattern, which picks up a few label-1 shots (up to 1,010 at p = 0.01). For those shots, the 16 recorded values cannot distinguish the two outcomes. This limits how well any model that sees only `syndrome_bits` can do on Part II Task A, and it is the reason for the `(experiment, syndrome, label)` key (§4.2).
