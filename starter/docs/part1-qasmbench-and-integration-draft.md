# Part I Design Report — Pipeline Integration & QASMBench Sections (Draft)

## 1. Source Discovery and Row Meanings

### 1.1 `qec_syndromes` (`bronze/source=qec_syndromes/syndromes_dataset.zip`)


### 1.2 `google_qec` (`bronze/source=google_qec/google-surface-code-curated.zip`)
The Google archive contains 76 archive members: a top-level `README.txt` and five experiment directories, each holding the same 15 members. Four are distance-3 experiments (`surface_code_bX_d3_r25_center_3_5`, `..._5_3`, `..._5_7`, `..._7_5`) and one is a distance-5 experiment (`surface_code_bX_d5_r25_center_5_5`). All use the X logical basis, 25 rounds, and 50,000 hardware shots. The directory name encodes the basis, distance, rounds, and processor-center coordinates, and every directory contains:

1. **`properties.yml`** (`278` bytes):
   - Experiment metadata: `basis`, `distance`, `rounds`, `shots`, `center_data_qubit_row/col`, `circuit_measurements`, `circuit_sweep_bits`, `circuit_detectors`, and `circuit_qubits`.
   - Distance-3 experiments declare `17` qubits, `209` measurements, `9` sweep bits, and `200` detectors per shot, while the distance-5 experiment declares `49` qubits, `625` measurements, `25` sweep bits, and `600` detectors per shot.
2. **Per-shot bit-packed files (Stim `b8`)** (`measurements.b8`, `sweep.b8`, `detection_events.b8`):
   - Records are byte-aligned and bits are little-endian within each byte. Expected file size is `ceil(bits_per_record / 8) * shots`, e.g., `27 * 50,000 = 1,350,000` bytes for `measurements.b8` in a distance-3 experiment.
   - Raw stabilizer **measurements** are hardware outcomes, whereas **detection events** are derived from changes between measurements across space and time. They are therefore kept as separate concepts. When `bits_per_record` is not a multiple of 8 (`209` and `625` measurement bits, `9` and `25` sweep bits), the unused padding bits of the last byte must be zero.
3. **Per-shot label and decoder files (`01`)** (`obs_flips_actual.01` plus four `obs_flips_predicted_by_{belief_matching, correlated_matching, pymatching, tensor_network_contraction}.01`):
   - One `0`/`1` line per shot (`50,000` lines, `100,000` bytes). `obs_flips_actual.01` is the actual logical outcome (a label), whereas the four other files are decoder predictions. A decoder mistake is the disagreement between the two, not a column in the source.
4. **Circuit and model files** (`circuit_ideal.stim`, `circuit_noisy.stim`, `circuit_detector_error_model.dem`, `pij_from_even_for_odd.dem`, `pij_from_odd_for_even.dem`, `layout.svg`):
   - Describe the circuit, noise model and chip layout. They are not needed for any required table and are ignored by Silver, though they stay in the Bronze archive.

- **What one row means**: In Silver, one `experiment` row is one experiment directory (described by `properties.yml`), and one `shot` row is one aligned hardware shot, assembled from the same line/record index across the 8 companion files (`measurements.b8`, `sweep.b8`, `detection_events.b8`, `obs_flips_actual.01`, and the 4 decoder `.01` files).
- **Candidate identifiers**: `experiment_id` (the directory name) uniquely identifies an experiment; `(experiment_id, shot_index)` uniquely identifies a shot; and `(basis, distance, rounds, center_row, center_col)` also identifies an experiment, since the same code at a different processor location is a different experiment.


### 1.3 `qasmbench` (`bronze/source=qasmbench/qasmbench-qec.zip`)
The QASMBench archive contains 19 archive members: top-level files (`LICENSE`, `NOTICE`, `README.md`, `qelib1.inc`) and three 5-qubit QEC benchmark directories under `small/`, each containing a `README.md`, circuit diagrams (`.png`), an OpenQASM circuit file (`<name>.qasm`), and its respective transpiled variant circuit (`<name>_transpiled.qasm`):

1. **`small/error_correctiond3_n5/`** (`error_correctiond3_n5.qasm` & `error_correctiond3_n5_transpiled.qasm`):
   - A 5-qubit distance-3 quantum error-correction circuit declaring `qreg q[5]; creg c[5];`.
   - Uses a single unified quantum register `q[5]` without separate ancilla (`a`) or classical syndrome (`syn`) registers or classical `if (...)` feedback statements. All 5 qubits are measured into `c[0..4]` at the end of the circuit.
2. **`small/qec_en_n5/`** (`qec_en_n5.qasm` & `qec_en_n5_transpiled.qasm`):
   - A 5-qubit bit-flip code encoder circuit declaring `qreg q[5]; creg c[5];` with 1-qubit gates, 10 two-qubit (`cx`) gates, and 5 terminal measurements into `c[0..4]`, also without separate ancilla/syndrome registers or mid-circuit conditional recovery.
3. **`small/qec_sm_n5/`** (`qec_sm_n5.qasm` & `qec_sm_n5_transpiled.qasm`):
   - A complete 3-data-qubit + 2-ancilla-qubit repetition-code syndrome measurement and recovery circuit declaring separate data (`qreg q[3]`), ancilla (`qreg a[2]`), data readout (`creg c[3]`), and syndrome (`creg syn[2]`) registers.
   - **Companion variants & packed/macro structures**:
     - In `qec_sm_n5.qasm` (`source` variant), stabilizer extraction is encapsulated inside a custom gate macro (`gate syndrome d1,d2,d3,a1,a2 { cx d1,a1; cx d2,a1; cx d2,a2; cx d3,a2; }`) invoked via `syndrome q[0],q[1],q[2],a[0],a[1];`, and measurements use whole-register vector syntax (`measure a -> syn;` and `measure q -> c;`).
     - In `qec_sm_n5_transpiled.qasm` (`transpiled` variant), the `syndrome` macro is inlined into four explicit `cx` statements, and measurements are unrolled into element-wise statements (`measure a[0] -> syn[0];`, `measure a[1] -> syn[1];`, etc.) interleaved with the `if(syn==...)` recovery gates.
   - **Candidate identifiers**: `circuit_id` (filename stem) uniquely identifies a circuit variant; `(circuit_id, check_id)` where `check_id = "{ancilla_qubit}_{sorted_data_qubits}_{syndrome_bit}"` uniquely identifies a stabilizer parity check; and `(circuit_id, target_qubit, condition_register, condition_value)` uniquely identifies a conditional recovery operation.

---

## 2. Architecture, Silver Layer, and Reproduction Commands

### 2.1 Four Stage Architecture (`bronze -> silver -> gold -> ml`)
We implemented the suggested architecture across MinIO object storage (`quantum-lake`) and PostgreSQL (`gold` schema), orchestrated by [`src/quantum_lake_student/runner.py`](../src/quantum_lake_student/runner.py):

1. **Bronze (`bronze/source=<source>/`) — Manifest-Verified Immutable Archives**:
   - [`stages/register_sources.py`](../src/quantum_lake_student/stages/register_sources.py) loads [`datasets/student-bundle/core/metadata/bundle-manifest.json`](../../datasets/student-bundle/core/metadata/bundle-manifest.json), verifies that all three mandatory Bronze archives exist without unexpected objects, checks exact byte lengths, computes SHA-256 over the actual archive bytes in MinIO to compare against `objects[].sha256`, and validates every ZIP member path (`archive_member_is_safe`) against path traversal (`..`), backslashes, and absolute paths.
   - Missing mandatory archives, hash/size mismatches, unsafe member paths, or missing required companion files raise an immediate error and stop the run.
2. **Silver (`silver/<source>/<table>.parquet`) — Checked, Source-Specific Parquet Tables**:
   - [`stages/prepare_data.py`](../src/quantum_lake_student/stages/prepare_data.py) parses and validates each source in memory into the six required Silver Parquet tables.
   - Shared evidence tables ([`results/part1/source_trace.parquet`](../results/part1/source_trace.parquet) and [`results/part1/data_issues.parquet`](../results/part1/data_issues.parquet)) are updated via [`results.replace_source_rows()`](../src/quantum_lake_student/results.py), which replaces only rows belonging to the active source so rerunning one source never overwrites teammates' rows or duplicates records.
3. **Gold (PostgreSQL `gold` schema) — All-or-Nothing Relational Model**:
   - [`stages/load_postgres.py`](../src/quantum_lake_student/stages/load_postgres.py) runs all three Gold loaders ([`gold/google_qec.py`](../src/quantum_lake_student/gold/google_qec.py), [`gold/qasmbench_gold.py`](../src/quantum_lake_student/gold/qasmbench_gold.py), [`gold/qec_syndromes.py`](../src/quantum_lake_student/gold/qec_syndromes.py)) inside one transaction (`with conn.transaction():`).
   - Rollback boundary: if any constraint or post-load reconciliation check fails in any source, the entire Gold update rolls back atomically, preserving the previous complete Gold version (verified by fault-injection tests in `tests/test_gold_*.py`).
4. **ML (`ml/*.parquet`) & Evidence (`results/part1/`)**:
   - [`stages/build_ml_tables.py`](../src/quantum_lake_student/stages/build_ml_tables.py) exports `ml/ml_syndrome_decoder_example.parquet` (`75,598` rows) and `ml/ml_google_decoder_example.parquet` (`250,000` rows) strictly from committed Gold SQL views (`v_ml_syndrome_decoder_example`, `v_ml_google_decoder_example`), validates their contracts, and executes the three SQL analyses into [`results/part1/analysis/`](../results/part1/analysis/).

### 2.2 QASMBench Silver Table Construction ([`sources/qasmbench_silver.py`](../src/quantum_lake_student/sources/qasmbench_silver.py))
`run_qasmbench_pipeline()` and `parse_qasm()` process `bronze/source=qasmbench/qasmbench-qec.zip` in four deterministic steps so that `source` and `transpiled` circuit variants produce identical semantic counts and stabilizer structures while retaining statement-level lineage:

1. **Archive Filtering & Hash Capture**:
   - Reads the Bronze ZIP archive from MinIO, computes its SHA-256 digest (`input_sha256`), and selects all `.qasm` members under `small/` (`6` files total), ignoring non-circuit members (`.png` diagrams, `README.md`, `LICENSE`, `NOTICE`, and top-level `qelib1.inc`).
2. **Statement Tokenization, Register Parsing & Custom Macro Extraction**:
   - Strips single-line `//` comments and parses `gate <name> <args> { <body> }` blocks into an in-memory macro dictionary (`custom_gates`).
   - Splits the OpenQASM program on `;` into 1-indexed sequential statements (`stmt_1`, `stmt_2`, ...) used as exact `record_locator` tokens in `results/part1/source_trace.parquet`.
   - Extracts `qreg <name>[<size>]` and `creg <name>[<size>]` declarations. If a `.qasm` file declares no `qreg` (or raises a syntax error), `parse_qasm()` records a `qasmbench.parse_error` (`Severity.ERROR`, `action="exclude"`) in `results/part1/data_issues.parquet` and excludes the circuit from Silver output.
3. **Timeline Unrolling & Recursive Macro Expansion (`linear_timeline`)**:
   - Iterates through `raw_statements` (`stmt_1..stmt_N`), ignoring non-operational headers/directives (`OPENQASM`, `include`, `qreg`, `creg`, `barrier`, `reset`, `gate`, `opaque`):
     - **Whole-register vs. element-wise `measure` statements**: Matches `measure <q_op> -> <c_op>`. When `<q_op>` is an unindexed register name (e.g., `measure a -> syn;` at `stmt_8` in `qec_sm_n5.qasm`), it unrolls the vector operation across the declared register size `qregs[q_op]` into individual qubit-to-bit measurement events (`a[0] -> syn[0]`, `a[1] -> syn[1]`), all tagged with `stmt_8`, and adds the expanded count to `measurement_count`.
     - **Classical conditional statements (`if (<reg> == <val>) <gate> <target>`)**: Extracts the classical condition register (`syn`), integer syndrome value (`condition_value`), recovery gate (`x`), and target qubit (`q[0]..q[2]`) directly into `conditional_correction.parquet` rows with statement locator `stmt_<idx>`.
     - **Gate operations & recursive macro expansion (`expand_instruction`)**: When a statement invokes a custom gate macro (e.g., `syndrome q[0],q[1],q[2],a[0],a[1];` at `stmt_7` in `qec_sm_n5.qasm`), `expand_instruction()` recursively substitutes formal macro parameters (`d1..a2`) with the call-site qubit arguments and emits the underlying primitive gates (`cx q[0],a[0]`, `cx q[1],a[0]`, `cx q[1],a[1]`, `cx q[2],a[1]`), each retaining the call-site locator (`stmt_7`). Every expanded 2-argument gate increments `two_qubit_gate_count` and is appended to `linear_timeline`.
4. **Stateful Stabilizer Parity-Check Reconstruction**:
   - Classifies quantum registers starting with `a` as ancilla registers (`ancilla_regs`), classical registers starting with `syn` as syndrome registers (`syndrome_regs`), and remaining quantum registers as data registers (`data_regs`).
   - Replays `linear_timeline` in execution order while maintaining per-ancilla interaction history (`ancilla_history[anc_target]`):
     - Whenever a 2-qubit gate couples a data qubit and an ancilla qubit, the data qubit is appended (in interaction order, deduplicated) to `ancilla_history[anc_target]["data_qubits"]` and the gate's statement locator is added to `stmt_locs`.
     - When an ancilla qubit `q` is subsequently measured into a syndrome bit `c`, if `ancilla_history[q]` contains entangled data qubits, a `stabilizer_check.parquet` row is emitted with deterministic `check_id = "{ancilla_qubit}_{sorted_data_qubits}_{syndrome_bit}"` and a `+`-joined statement locator combining all contributing entangling and measurement statements in numerical order (e.g., `stmt_7+stmt_8` in `qec_sm_n5.qasm` vs. `stmt_7+stmt_8+stmt_11` in `qec_sm_n5_transpiled.qasm`), after which `ancilla_history[q]` is reset.
   - Because `error_correctiond3_n5` and `qec_en_n5` declare only a single unified `qreg q[5]` and `creg c[5]` (no ancilla `a*` or syndrome `syn*` registers), they contribute rows only to `circuit.parquet`, whereas `qec_sm_n5` and `qec_sm_n5_transpiled` each emit `2` stabilizer checks (`4` total) and `3` conditional corrections (`6` total).

#### `silver/qasmbench/circuit.parquet` (6 rows)
| Column | Arrow Type | Derivation Rule | Example (`qec_sm_n5.qasm`) |
| --- | --- | --- | --- |
| `source_record_id` | `string` | `"qasmbench:<archive_member>"` | `"qasmbench:small/qec_sm_n5/qec_sm_n5.qasm"` |
| `circuit_id` | `string` | File stem of the `.qasm` archive member | `"qec_sm_n5"` |
| `benchmark_name` | `string` | Stem with `_transpiled` and `_n<digits>` suffixes stripped | `"qec_sm"` |
| `variant` | `string` | `"transpiled"` if `"transpiled"` in stem, else `"source"` | `"source"` |
| `register_declarations` | `string` | Key-sorted JSON `{"cregs": {...}, "qregs": {...}}` | `'{"cregs": {"c": 3, "syn": 2}, "qregs": {"a": 2, "q": 3}}'` |
| `qubit_count` | `int32` | Sum of declared `qreg` sizes (`sum(qregs.values())`) | `5` |
| `measurement_count` | `int32` | Expanded executed `measure` operations | `5` |
| `two_qubit_gate_count` | `int32` | Expanded executed 2-qubit gates (after macro expansion) | `4` |

#### `silver/qasmbench/stabilizer_check.parquet` (4 rows)
| Column | Arrow Type | Derivation Rule | Example (`qec_sm_n5.qasm`, check 1) |
| --- | --- | --- | --- |
| `source_record_id` | `string` | `"qasmbench:<archive_member>:<stmt_locators>"` (`+`-joined) | `"qasmbench:small/qec_sm_n5/qec_sm_n5.qasm:stmt_7+stmt_8"` |
| `circuit_id` | `string` | Parent circuit identifier (`circuit.circuit_id`) | `"qec_sm_n5"` |
| `check_id` | `string` | `"{ancilla_qubit}_{sorted_data_qubits}_{syndrome_bit}"` | `"a[0]_q[0]-q[1]_syn[0]"` |
| `ancilla_qubit` | `string` | Ancilla qubit (`a*`) entangled with data qubits and measured | `"a[0]"` |
| `data_qubits` | `list<string>` | Data qubits (`q*`) interacting via 2-qubit gates before measurement | `["q[0]", "q[1]"]` |
| `syndrome_bit` | `string` | Classical bit (`syn*`) receiving the ancilla measurement | `"syn[0]"` |

#### `silver/qasmbench/conditional_correction.parquet` (6 rows)
| Column | Arrow Type | Derivation Rule | Example (`qec_sm_n5.qasm`, rule 1) |
| --- | --- | --- | --- |
| `source_record_id` | `string` | `"qasmbench:<archive_member>:stmt_<idx>"` | `"qasmbench:small/qec_sm_n5/qec_sm_n5.qasm:stmt_9"` |
| `circuit_id` | `string` | Parent circuit identifier (`circuit.circuit_id`) | `"qec_sm_n5"` |
| `condition_register` | `string` | Classical register tested in `if (<reg> == <val>)` | `"syn"` |
| `condition_value` | `int64` | Integer syndrome value activating the recovery gate | `1` |
| `gate` | `string` | Recovery gate operation name | `"x"` |
| `target_qubit` | `string` | Target data qubit acted on by the recovery gate | `"q[0]"` |

### 2.3 Reproduction Commands
From the repository root:
- **Bootstrap clean environment**: `make bootstrap` (or `make reset-platform && make bootstrap` for a clean wipe)
- **Run Part I pipeline**: `make run` (safe to run repeatedly; verified idempotent)
- **Run Part II ML pipeline**: `make train`
- **Run automated test suite (124 tests)**: `make test`

### 2.5 Google Silver Table Construction ([`sources/google_qec.py`](../src/quantum_lake_student/sources/google_qec.py))
`run()` and `process_experiment_dir()` process `bronze/source=google_qec/google-surface-code-curated.zip` in four deterministic steps so that every accepted experiment yields one `experiment` row and exactly `shots` aligned `shot` rows:

1. **Archive Reading & Hash Capture**:
   - Reads the Bronze ZIP from MinIO, computes its SHA-256 digest (`input_sha256`), validates member paths with `safe_member_names()`, and opens the archive in memory (`zipfile.Path`) without extracting it to disk. Every directory containing a `properties.yml` is treated as one experiment, in sorted order.
2. **Companion-File and Metadata Checks**:
   - `verify_companion_files()` requires all 9 files used by Silver (`properties.yml`, the three `b8` files, `obs_flips_actual.01`, and the 4 decoder `.01` files). `load_properties()` checks that the directory name matches `surface_code_b<basis>_d<distance>_r<rounds>_center_<row>_<col>`, that `properties.yml` parses to a mapping with all required keys, and that the directory name and `properties.yml` agree on basis, distance, rounds, and center.
3. **Length, Padding & Value Validation**:
   - For each `b8` file, `_read_packed_matrix()` computes the expected byte length from `properties.yml` (`b8_record_bytes(bits) * shots`) and checks that the unused padding bits in the last byte of every record are zero. For each `01` file, `_read_01_column()` checks that all values are `0`/`1` and that the row count equals `shots`.
   - If any check fails for an experiment, the whole experiment is skipped (no experiment or shot rows, no partial shots) and a `QualityFinding` per failed check is written to `results/part1/data_issues.parquet`.
4. **Shot Assembly & Packing**:
   - For `shot_index` in `0..shots-1`, the same record index is read from all 8 companion files. Bits are re-packed with `pack_bits()` into Stim `b8` bytes (`measurement_bits`, `sweep_bits`, `detector_bits`), `detector_event_count = sum(detector_bits)`, and the actual flip and the four decoder predictions become booleans. `sweep_bits` is empty if `circuit_sweep_bits = 0`.
   - Each experiment gets one trace row (`archive_member = "<experiment_id>/properties.yml"`) and each shot gets 8 trace rows in `results/part1/source_trace.parquet`, one per companion member, all with `record_locator = "<experiment_id>:shot=<shot_index>"`.

#### `silver/google_qec/experiment.parquet` (5 rows)
| Column | Arrow Type | Derivation Rule | Example (`surface_code_bX_d3_r25_center_3_5`) |
| --- | --- | --- | --- |
| `source_record_id` | `string` | `"google_qec:<experiment_id>:properties"` | `"google_qec:surface_code_bX_d3_r25_center_3_5:properties"` |
| `experiment_id` | `string` | Experiment directory name | `"surface_code_bX_d3_r25_center_3_5"` |
| `basis` | `string` | `basis` in `properties.yml` | `"X"` |
| `distance` | `int32` | `distance` in `properties.yml` | `3` |
| `rounds` | `int32` | `rounds` in `properties.yml` | `25` |
| `shots` | `int64` | `shots` in `properties.yml` | `50000` |
| `center_row` / `center_col` | `int32` | `center_data_qubit_row` / `center_data_qubit_col` | `3` / `5` |
| `measurement_count` | `int32` | `circuit_measurements` | `209` |
| `detector_count` | `int32` | `circuit_detectors` | `200` |

#### `silver/google_qec/shot.parquet` (250,000 rows)
| Column | Arrow Type | Derivation Rule | Example (`...center_3_5`, shot 1) |
| --- | --- | --- | --- |
| `source_record_id` | `string` | `"google_qec:<experiment_id>:shot=<shot_index>"` | `"google_qec:surface_code_bX_d3_r25_center_3_5:shot=1"` |
| `experiment_id` | `string` | Parent experiment (`experiment.experiment_id`) | `"surface_code_bX_d3_r25_center_3_5"` |
| `shot_index` | `int64` | Zero-based record index in the companion files | `1` |
| `measurement_bits` / `sweep_bits` / `detector_bits` | `binary` | Packed Stim `b8` record of the shot | `27` / `2` / `25` bytes (distance 3) |
| `detector_event_count` | `int32` | Number of set bits in `detector_bits` | `36` |
| `actual_observable_flip` | `bool` | Line `shot_index` of `obs_flips_actual.01` | `True` |
| `<decoder>_prediction` (4 columns) | `bool` | Line `shot_index` of `obs_flips_predicted_by_<decoder>.01` | one per decoder |

---

## 3. Gold Tables, Relationships, Keys, Constraints, and Indexes

### 3.1 `qec_syndromes` Gold Tables ([`gold/sql/qec_syndromes/01_schema.sql`](../src/quantum_lake_student/gold/sql/qec_syndromes/01_schema.sql))


### 3.2 `google_qec` Gold Tables ([`gold/sql/google_qec/01_schema.sql`](../src/quantum_lake_student/gold/sql/google_qec/01_schema.sql))
The Gold design for the Google data splits the two Silver tables by how they are used. The `shot` Silver table is divided into `google_shot` (the columns every analysis and the ML export need), `google_shot_raw` (the bulky raw bits), and `google_shot_prediction` (its four decoder-prediction columns, normalized to one row per decoder per shot, with a small `decoder` lookup table). In addition, `google_detector_position_stat` is derived at load time from the packed detector bits. Specifically the Gold design for the Google data is as follows:

| Gold Table | One row represents... | Primary Key | Foreign Keys, Constraints & Indexes | Rows |
| --- | --- | --- | --- | ---: |
| `google_experiment` | One hardware experiment (code distance, basis, rounds, processor location). | `(experiment_id)` | `UNIQUE (source_record_id)`, `UNIQUE (basis, distance, rounds, center_row, center_col)`, `CHECK (basis IN ('X', 'Z'))`, `CHECK (distance >= 3 AND distance % 2 = 1)`, `CHECK (rounds > 0)`, `CHECK (shots > 0)`, `CHECK (measurement_count > 0)`, `CHECK (detector_count > 0)`, index on `(distance)` | 5 |
| `decoder` | One decoder whose predictions were supplied with the data. | `(decoder_id)` | `UNIQUE (decoder_name)` | 4 |
| `google_shot` | One aligned hardware shot: packed detector events and the actual logical flip. | `(experiment_id, shot_index)` | `FK (experiment_id) -> google_experiment`, `UNIQUE (source_record_id)`, `CHECK (shot_index >= 0)`, `CHECK (detector_event_count = bit_count(detector_bits))` | 250,000 |
| `google_shot_raw` | The raw measurement and sweep bits of one shot (strictly 1:1 with `google_shot`). | `(experiment_id, shot_index)` | `FK (experiment_id, shot_index) -> google_shot ON DELETE CASCADE` | 250,000 |
| `google_shot_prediction` | One decoder's predicted logical flip for one shot. | `(experiment_id, shot_index, decoder_id)` | `FK (experiment_id, shot_index) -> google_shot ON DELETE CASCADE`, `FK (decoder_id) -> decoder`, index on `(decoder_id, experiment_id)` | 1,000,000 |
| `google_detector_position_stat` | How often one detector position fired in one experiment. | `(experiment_id, detector_index)` | `FK (experiment_id) -> google_experiment`, `CHECK (detector_index >= 0)`, `CHECK (shots_observed > 0)`, `CHECK (fired_count BETWEEN 0 AND shots_observed)` | 1,400 |

Four views sit on top of these tables: `v_google_decoder_outcome` (one row per shot and decoder with the derived `is_logical_error`), `v_google_shot_predictions_wide` (the four predictions side by side), `v_google_example_lookup` (ML `example_id` $\to$ shot), and `v_ml_google_decoder_example` (the ML export).


### 3.3 `qasmbench` Gold Tables ([`gold/sql/qasmbench/01_schema.sql`](../src/quantum_lake_student/gold/sql/qasmbench/01_schema.sql))
The design of the Gold tables for the QASMBench dataset, follows mostly the tables in the silver part of the pipeline with the definition of the necessary constraints, keys and relationships. Additionally, in order to have tables with columns that represent atomic data, we normalize 2 columns from the silver tables into 2 new gold tables. First the `circuit_register` Gold table is derived from the `register_declarations` column of the `circuit.parquet` Silver table, and second the `stabilizer_data_qubit` Gold table derived from the `data_qubits` column of the `stabilizer_check.parquet` Silver table. Specifically the Gold design for the QASMBench data is as follows:

| Gold Table | One row represents... | Primary Key | Foreign Keys, Constraints & Indexes | Rows |
| --- | --- | --- | --- | ---: |
| `circuit` | One quantum benchmark circuit variant (`source` or `transpiled`) with its qubit, measurement, and two-qubit gate counts. | `(circuit_id)` | `UNIQUE (benchmark_name, qubit_count, variant)`, `UNIQUE (source_record_id)`, `CHECK (variant IN ('source', 'transpiled'))`, `CHECK (qubit_count > 0)`, `CHECK (measurement_count >= 0)`, `CHECK (two_qubit_gate_count >= 0)` | 6 |
| `circuit_register` | One quantum (`qreg`) or classical (`creg`) register allocated in a circuit. | `(circuit_id, register_name)` | `FK (circuit_id) -> circuit(circuit_id) ON DELETE CASCADE`, `CHECK (register_type IN ('qreg', 'creg'))`, `CHECK (size > 0)` | 16 |
| `stabilizer_check` | One stabilizer parity check mapping an ancilla qubit to a classical syndrome bit in a circuit. | `(circuit_id, check_id)` | `FK (circuit_id) -> circuit(circuit_id) ON DELETE CASCADE` | 4 |
| `stabilizer_data_qubit` | One data qubit participating in one stabilizer parity check (many-to-many association table). | `(circuit_id, check_id, data_qubit)` | `FK (circuit_id, check_id) -> stabilizer_check ON DELETE CASCADE`, `FK (circuit_id) -> circuit ON DELETE CASCADE` | 8 |
| `conditional_correction` | One classical syndrome-conditioned feedforward recovery gate applied to a target data qubit. | `(circuit_id, target_qubit, condition_register, condition_value)` | `FK (circuit_id) -> circuit(circuit_id) ON DELETE CASCADE`, `UNIQUE (source_record_id)`, `CHECK (condition_value >= 0)` | 6 |

---

## 4. Design Decisions, Storage Trade-offs, and Rejected Relationship

### 4.1 QASMBench Gold Design Decisions
- **Normalizing JSON register data and data-qubit lists into tables**: Decomposing `register_declarations` into `circuit_register` and `data_qubits` into `stabilizer_data_qubit` lets SQL enforce referential integrity and directly join a data qubit (`stabilizer_data_qubit.data_qubit`) to its recovery rule (`conditional_correction.target_qubit`) in Question 3 without fragile string/array parsing in SQL.
- **Primary Keys, Uniqueness Constraints and identified relationships**:
   - Every QASMBench Gold table uses deterministic natural or composite domain keys, ensuring repeated loads produce identical keys without duplicates:
     - **`circuit` — `PRIMARY KEY (circuit_id)` + `UNIQUE (benchmark_name, qubit_count, variant)`**: `circuit_id` (e.g., `qec_sm_n5`, `qec_sm_n5_transpiled`) is the natural parent key referenced by all child tables. We additionally enforce `UNIQUE (benchmark_name, qubit_count, variant)` because each QASMBench circuit family (`error_correctiond3`, `qec_en`, `qec_sm`) at a given qubit count (`n=5`) has at most one `source` and one `transpiled` representation, plus `UNIQUE (source_record_id)` to guarantee 1-to-1 lineage to the `.qasm` archive member.
     - **`circuit_register` — `PRIMARY KEY (circuit_id, register_name)`**: Register identifiers live in a single per-circuit namespace. A `qreg` and a `creg` in the same circuit cannot share a `register_name`. Thus `(circuit_id, register_name)` is the minimal natural key (without needing `register_type` in the key).
     - **`stabilizer_check` — `PRIMARY KEY (circuit_id, check_id)`**: `check_id` (`"{ancilla_qubit}_{sorted_data_qubits}_{syndrome_bit}"`, e.g., `a[0]_q[0]-q[1]_syn[0]`) is intentionally identical between `qec_sm_n5` (`source`) and `qec_sm_n5_transpiled` (`transpiled`), since both variants implement the exact same logical parity checks. Therefore, `check_id` alone is not globally unique across the table; the composite key `(circuit_id, check_id)` uniquely identifies each check per circuit variant while making it easy to join `source` and `transpiled` variants on `check_id` to verify transpilation equivalence.
     - **`stabilizer_data_qubit` — `PRIMARY KEY (circuit_id, check_id, data_qubit)`**: In a stabilizer parity check, each data qubit participates once (`cx q[i], a[j]`), making the 3-tuple `(circuit_id, check_id, data_qubit)` the minimal composite key for the $M:N$ association table, backed by a composite foreign key `FOREIGN KEY (circuit_id, check_id) REFERENCES stabilizer_check(circuit_id, check_id) ON DELETE CASCADE`.
     - **`conditional_correction` — `PRIMARY KEY (circuit_id, target_qubit, condition_register, condition_value)` + `UNIQUE (source_record_id)`**: A single classical syndrome value can trigger gates on multiple qubits, and a single target qubit can be corrected under multiple syndrome conditions. The 4-tuple `(circuit_id, target_qubit, condition_register, condition_value)` uniquely identifies each conditional recovery rule on a target qubit, while `UNIQUE (source_record_id)` enforces 1-to-1 traceability to the originating QASM `stmt_<idx>`.


### 4.1.1 Google Gold Design Decisions
- **Decoder mistakes are derived, not stored**: A decoder mistake is `predicted_flip <> actual_observable_flip`, computed in the view `v_google_decoder_outcome` (`is_logical_error`). Storing it would let it disagree with the two columns it comes from, and it keeps the actual label and the prediction as separate facts.
- **Predictions are one row per decoder per shot**: `google_shot_prediction` (`PRIMARY KEY (experiment_id, shot_index, decoder_id)`) makes adding a decoder new data rather than a schema change. It costs `1M` rows (about 160 MB) and a pivot for the ML export (`v_google_shot_predictions_wide`), which is acceptable at this data size.
- **Raw bits are kept in their own table**: Most queries only need `google_shot`, so the large `measurement_bits` and `sweep_bits` live in `google_shot_raw` (1:1, same key). Raw measurements and derived detector events stay separate concepts.
- **Detector events are stored packed, plus a per-position summary table**: Each shot keeps one packed `bytea` (`detector_bits`), plus `google_detector_position_stat` (`1,400` rows). We measured the alternative of one row per fired detector: about `10.5M` rows and `1.9 GB`, roughly 200x larger than the packed column. The packed bytes are also exactly what the ML export needs. The downside is that finding shots where one specific detector fired requires scanning with `get_bit`, which is fine at this size. Stim `b8` and PostgreSQL `get_bit` number bits the same way, so the position statistics are computed straight from the packed bytes (tested in `test_position_stats_use_b8_bit_order`).
- **Primary Keys, Uniqueness Constraints and identified relationships**:
   - Keys come from the source (`experiment_id`, `shot_index`), so repeated loads produce identical keys.
     - **`google_experiment` — `PRIMARY KEY (experiment_id)` + `UNIQUE (basis, distance, rounds, center_row, center_col)`**: The same code distance, rounds and basis at a different processor location is a different experiment (the four distance-3 experiments differ only in location), so location is part of the natural identity. `UNIQUE (source_record_id)` guarantees 1-to-1 lineage to `properties.yml`.
     - **`google_shot` — `PRIMARY KEY (experiment_id, shot_index)`**: `shot_index` is only unique within an experiment. `CHECK (detector_event_count = bit_count(detector_bits))` keeps the duplicated count consistent with the stored bits.
     - **`google_shot_prediction` — `PRIMARY KEY (experiment_id, shot_index, decoder_id)`**: Each decoder predicts exactly once per shot, which is enforced after load (see Section 5.1).
- **All-or-nothing load**: DDL, `COPY`, inserts, views, and the checks in `03_checks.sql` run in one transaction and only touch `google_*`, `v_google_*`, `v_ml_google_*`, and `decoder` objects, so other sources in the `gold` schema are never affected.

### 4.2 Investigated and Rejected Relationships (with Evidence)
- **Google experiments $\leftrightarrow$ QASMBench circuits (rejected)**: We did not link the Google experiments to the QASMBench circuits. The two sources share no field (no id, qubit layout, or device), the QASMBench circuits are 5-qubit textbook examples whereas the Google experiments are 17-qubit (distance 3) and 49-qubit (distance 5) surface codes, and the assignment states that the circuits do not identify the ones used by the Google experiments. Any join would be a guess, so the sources remain complementary rather than joinable.
- **Storing a decoder-mistake flag in `google_shot_prediction` (rejected)**: It would duplicate information derivable from `predicted_flip` and `actual_observable_flip` and could drift from them, so it is a view instead (Section 4.1.1).


---

## 5. Data Quality Findings and Count Reconciliation

### 5.1 Distinguishing Structural Failures from Record-Level Issues 
- **Structural / Archive Checks (fail-fast)**: Missing mandatory Bronze archives, SHA-256/byte mismatches against `bundle-manifest.json`, unsafe archive member paths (`..`, absolute paths), or missing required companion files abort the run immediately.
- **Record-Level Checks (`results/part1/data_issues.parquet`)**:
  - **QASMBench (`qasmbench.parse_error`)**: Validates that every `.qasm` file is valid OpenQASM and declares at least one `qreg`. Malformed circuits are excluded (`action = 'exclude'`) and recorded in `data_issues.parquet` (tested in [`tests/test_qasmbench_silver.py`](../tests/test_qasmbench_silver.py)). All 6 supplied circuits pass (`0` issues).
  - **`qec_syndromes`**:
  - **`google_qec`**: Each experiment is validated as a unit in Silver and rejected as a whole (`action = 'rejected'`) if any check fails, so Gold never sees partial experiments (tested in [`tests/test_google_qec.py`](../tests/test_google_qec.py)). Rules: `google_qec.missing_companion_file`, `unexpected_directory_name`, `invalid_properties_file`, `properties_missing_keys`, `directory_properties_mismatch`, `b8_length_mismatch`, `b8_padding_not_zero`, `invalid_01_value`, and `01_row_count_mismatch`. All 5 experiments pass (`0` issues). Gold repeats the structural checks inside the load transaction ([`03_checks.sql`](../src/quantum_lake_student/gold/sql/google_qec/03_checks.sql)): shot count equals `google_experiment.shots`, every shot has one raw row and one prediction per decoder (4), correct `detector_bits` and `measurement_bits` byte lengths, zero padding bits, and per-position firing counts summing to `detector_event_count`. Any violation rolls back the whole Gold update (tested in [`tests/test_gold_google_qec.py`](../tests/test_gold_google_qec.py)). One issue found during development: Silver initially stored bits one byte per bit instead of packed `b8`, which was fixed with `pack_bits`.


### 5.2 Bronze $\to$ Silver $\to$ Gold $\to$ ML Row Count Reconciliation ([`results/part1/row_counts.json`](../results/part1/row_counts.json))

| Layer | Table / Object | Read | Accepted / Loaded | Rejected | Reconciled |
| --- | --- | ---: | ---: | ---: | :---: |
| **Silver** | `qec_syndromes.syndrome_observation` | 75,598 | 75,598 (70M weighted) | 0 | Yes |
| **Silver** | `google_qec.experiment` | 5 | 5 | 0 | Yes |
| **Silver** | `google_qec.shot` | 250,000 | 250,000 | 0 | Yes |
| **Silver** | `qasmbench.circuit` | 6 | 6 | 0 | Yes |
| **Silver** | `qasmbench.stabilizer_check` | 4 | 4 | 0 | Yes |
| **Silver** | `qasmbench.conditional_correction` | 6 | 6 | 0 | Yes |
| **Gold** | `gold.simulated_experiment` / `syndrome_pattern` / `syndrome_observation` | — | 7 / 31,941 / 75,598 | — | Yes |
| **Gold** | `gold.decoder` / `google_experiment` / `google_shot` / `google_shot_raw` / `google_shot_prediction` / `google_detector_position_stat` | — | 4 / 5 / 250k / 250k / 1M / 1,400 | — | Yes |
| **Gold** | `gold.circuit` / `circuit_register` / `stabilizer_check` / `stabilizer_data_qubit` / `conditional_correction` | — | 6 / 16 / 4 / 8 / 6 | — | Yes |
| **ML** | `ml/ml_syndrome_decoder_example.parquet` | 75,598 | 75,598 | 0 | Yes |
| **ML** | `ml/ml_google_decoder_example.parquet` | 250,000 | 250,000 | 0 | Yes |

---

## 6. End-to-End Source Traces ([`results/part1/trace_examples.json`](../results/part1/trace_examples.json))

Both traces in [`results/part1/trace_examples.json`](../results/part1/trace_examples.json) select deterministic `test`-split examples so that every link in the chain—from a concrete Part II prediction in [`results/part2/predictions.parquet`](../results/part2/predictions.parquet) (`P1-03`) through ML, Gold, and Silver back to the Bronze archive bytes—resolves:

1. **Syndrome Trace (`example_id = "a47305529fb5dded09d25a1c5d21d1da44d3b7ca6ca24673918beec3fd6317b1"`)**:
   - **Part II Prediction (`results/part2/predictions.parquet`)**:
     - `model_id = "task_a_weighted_logistic"`, `split = "test"` $\to$ `label = True`, `prediction = True`, `probability = 0.148709` (and `model_id = "task_a_weighted_prior"`, `split = "test"` $\to$ `prediction = False`, `probability = 0.042582`).
   - **ML (`ml/ml_syndrome_decoder_example.parquet`)**: `data_split = "test"`, `logical_error_label = True`, `sample_weight = 141407`.
   - **Gold (`gold` schema)**: `syndrome_observation.observation_id = example_id` $\to$ `experiment_id = "d-3_pfr-0.005000_nb-10M"`, `syndrome_id = 6468` (`syndrome_bits_hex = "00000100000000000000000000000000"`, `fired_count = 1`), `quantity = 141407`.
   - **Silver (`silver/qec_syndromes/syndrome_observation.parquet`)**: `source_record_id = "qec_syndromes:d-3_pfr-0.005000_nb-10M.csv:line=3"`.
   - **Bronze (`results/part1/source_trace.parquet`)**: `bronze_object = "bronze/source=qec_syndromes/syndromes_dataset.zip"`, `archive_member = "d-3_pfr-0.005000_nb-10M.csv"`, `record_locator = "line=3"`, `input_sha256 = "bdfce36a..."`.

2. **Google Trace (`example_id = "75f380e1bb46b4eaddabdc73f2bbbc1e102c00e127e8e6532b31f5c47efeb344"`)**:
   - **Part II Prediction (`results/part2/predictions.parquet`)**:
     - `model_id = "task_b_d3_combined_logistic"`, `split = "test"` $\to$ `label = True`, `prediction = True`, `probability = 0.554865` (also present for `task_b_d3_majority_prior`, all 4 supplied decoders, and `task_c_d3_raw_detector_mlp` with `probability = 0.805783`).
   - **ML (`ml/ml_google_decoder_example.parquet`)**: `data_split = "test"`, `actual_observable_flip = True`, `detector_event_count = 36`.
   - **Gold (`gold` schema)**: Resolved via `v_google_example_lookup` to `experiment_id = "surface_code_bX_d3_r25_center_3_5"`, `shot_index = 1` across `google_shot`, `google_experiment`, `google_shot_raw`, and 4 rows in `google_shot_prediction`.
   - **Silver (`silver/google_qec/shot.parquet` & `experiment.parquet`)**: `source_record_id = "google_qec:surface_code_bX_d3_r25_center_3_5:shot=1"` and `"google_qec:surface_code_bX_d3_r25_center_3_5:properties"`.
   - **Bronze (`results/part1/source_trace.parquet`)**: Resolves to all **8 aligned companion archive members** in `bronze/source=google_qec/google-surface-code-curated.zip` (`measurements.b8`, `sweep.b8`, `detection_events.b8`, `obs_flips_actual.01`, and the 4 decoder `.01` files) at `record_locator = "surface_code_bX_d3_r25_center_3_5:shot=1"`, `input_sha256 = "5d6a24f8..."`.

---

## 7. Part I SQL Analyses and Interpretations

### 7.1 Question 1 — Simulated Syndromes by Physical Fault Rate
- **SQL**: [`gold/sql/qec_syndromes/analysis/q1_syndrome_by_fault_rate.sql`](../src/quantum_lake_student/gold/sql/qec_syndromes/analysis/q1_syndrome_by_fault_rate.sql) and [`q1_syndrome_by_fired_count.sql`](../src/quantum_lake_student/gold/sql/qec_syndromes/analysis/q1_syndrome_by_fired_count.sql)
- **Outputs**: [`results/part1/analysis/q1_syndrome_by_fault_rate.csv`](../results/part1/analysis/q1_syndrome_by_fault_rate.csv) and [`q1_syndrome_by_fired_count.csv`](../results/part1/analysis/q1_syndrome_by_fired_count.csv)


### 7.2 Question 2 — Google Decoder Error Rates by Distance and Location
- **SQL**: [`gold/sql/google_qec/analysis/google_decoder_error_rates.sql`](../src/quantum_lake_student/gold/sql/google_qec/analysis/google_decoder_error_rates.sql), [`google_decoder_error_overlap.sql`](../src/quantum_lake_student/gold/sql/google_qec/analysis/google_decoder_error_overlap.sql), and [`google_detector_hotspots.sql`](../src/quantum_lake_student/gold/sql/google_qec/analysis/google_detector_hotspots.sql)
- **Outputs**: [`results/part1/analysis/google_decoder_error_rates.json`](../results/part1/analysis/google_decoder_error_rates.json), [`google_decoder_error_overlap.json`](../results/part1/analysis/google_decoder_error_overlap.json), and [`google_detector_hotspots.json`](../results/part1/analysis/google_detector_hotspots.json)


**Interpretation**:
The actual logical flip is close to a coin flip (`49.3%` of shots at distance 3, `50.0%` at distance 5), so a decoder that learned nothing would be wrong about half the time.
- **Error rates**: Pooled over the four distance-3 experiments (200,000 shots), the decoders are wrong on `40.0%` (`belief_matching`), `39.7%` (`tensor_network_contraction`), `41.9%` (`correlated_matching`), and `43.4%` (`pymatching`) of shots. At distance 5 (50,000 shots) they are wrong on `40.1%`, `39.6%`, `42.6%`, and `45.1%`. All beat the raw flip rate by only a modest margin, and decoders are only a few points apart. Between distance-3 locations the rates differ by up to about 3 points.
- **Overlap**: At distance 3, all four decoders are right on `30.3%` of shots and all four are wrong on `14.3%` (distance 5: `24.4%` and `10.9%`). The remaining `55%` of shots (`65%` at distance 5) split the decoders, so their mistakes only partly overlap, which a combined decoder could use.

### 7.3 Question 3 — Repetition-Code Circuit Mapping (4-Table Gold Join)
- **SQL**: [`gold/sql/qasmbench/analysis/question_3_repetition_code.sql`](../src/quantum_lake_student/gold/sql/qasmbench/analysis/question_3_repetition_code.sql) (grouped by data qubit, 3 rows)
- **Output**: [`results/part1/analysis/question_3_repetition_code.csv`](../results/part1/analysis/question_3_repetition_code.csv)

The query joins **4 Gold tables** (`circuit c`, `stabilizer_check s`, `stabilizer_data_qubit dq`, and `conditional_correction cc`) on `c.circuit_id = 'qec_sm_n5'`:

| `data_qubit` | `parity_ancillas` (`s.ancilla_qubit`) | `syndrome_bits` (`s.syndrome_bit`) | `condition_register` | `condition_value` | `recovery_gate` |
| --- | --- | --- | --- | ---: | --- |
| `q[0]` | `a[0]` | `syn[0]` | `syn` | 1 | `x` |
| `q[1]` | `a[0], a[1]` | `syn[0], syn[1]` | `syn` | 3 | `x` |
| `q[2]` | `a[1]` | `syn[1]` | `syn` | 2 | `x` |

**Interpretation**:
The 4-table relational join reconstructs the complete syndrome decoding lookup table of the 3-qubit bit-flip repetition code:
- **Outer qubit `q[0]`**: Checked only by ancilla `a[0]`, which measures into `syn[0]` (least-significant bit, binary weight $2^0 = 1$). When `syn == 1` ($\text{syn} = 01_2$), an `x` gate is applied to `q[0]`.
- **Outer qubit `q[2]`**: Checked only by ancilla `a[1]`, which measures into `syn[1]` (most-significant bit, binary weight $2^1 = 2$). When `syn == 2` ($\text{syn} = 10_2$), an `x` gate is applied to `q[2]`.
- **Middle qubit `q[1]`**: Shared between both parity checks (`a[0]` and `a[1]`), feeding both `syn[0]` and `syn[1]` (combined binary weight $1 + 2 = 3$). When `syn == 3` ($\text{syn} = 11_2$), an `x` gate is applied to `q[1]`.
