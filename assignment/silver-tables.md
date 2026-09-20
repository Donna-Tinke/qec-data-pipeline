# Minimum Silver tables

Silver is the checked, source-specific layer of the assignment. These six
tables are the minimum shared hand-off format. You may add columns or tables,
but do not join different data sources in Silver.

The listed types are logical Parquet/Arrow types. Choose clear nullable rules
and document any additional fields. Every table must include the stable
`source_record_id` described below.

## Shared tracing rule

`source_record_id` is a stable string that identifies the source record from
which a Silver row was produced. It must not depend on the run time or row order
of a database load.

Store the full source details in `results/part1/source_trace.parquet`, not in
every Silver table. That tracing table contains at least:

| Column | Meaning |
| --- | --- |
| `source_record_id` | stable source-record identifier |
| `source_name` | `qec_syndromes`, `google_qec`, or `qasmbench` |
| `bronze_object` | supplied object/archive name |
| `archive_member` | member path inside an archive, if applicable |
| `record_locator` | CSV row, shot number, circuit statement, or other repeatable position |
| `input_sha256` | hash of the Bronze object |

More than one trace row may share a `source_record_id` when a logical record,
such as a Google shot, is assembled from aligned companion files.

## 1. Syndrome observations

Path: `silver/qec_syndromes/syndrome_observation.parquet`

One row represents one original aggregate CSV row.

| Column | Type | Meaning |
| --- | --- | --- |
| `source_record_id` | string | stable link to the original CSV record |
| `experiment_id` | string | stable identifier for the fault-rate experiment |
| `physical_fault_rate` | float64 | value obtained from the source filename |
| `syndrome_bits` | binary | exactly 16 one-byte binary values, round first and check second |
| `round_count` | int32 | must equal 4 |
| `check_count` | int32 | must equal 4 |
| `logical_error_label` | bool | source `labels` value |
| `quantity` | int64 | number of physical observations represented by the row; greater than zero |

Do not expand `quantity` into repeated rows. The same syndrome may validly
occur with both labels.

## 2. Google experiments

Path: `silver/google_qec/experiment.parquet`

One row represents one hardware experiment directory.

| Column | Type | Meaning |
| --- | --- | --- |
| `source_record_id` | string | stable link to the experiment metadata source |
| `experiment_id` | string | stable experiment identifier |
| `basis` | string | logical measurement basis |
| `distance` | int32 | code distance |
| `rounds` | int32 | number of QEC rounds |
| `shots` | int64 | number of aligned hardware shots |
| `center_row` | int32 | processor-location row coordinate |
| `center_col` | int32 | processor-location column coordinate |
| `measurement_count` | int32 | measurement bits per shot |
| `detector_count` | int32 | detector bits per shot |

## 3. Google shots

Path: `silver/google_qec/shot.parquet`

One row represents one aligned hardware shot. Validate all companion-file
lengths before publishing the row.

| Column | Type | Meaning |
| --- | --- | --- |
| `source_record_id` | string | stable identifier for the aligned source shot |
| `experiment_id` | string | link to the Silver experiment |
| `shot_index` | int64 | zero-based shot number |
| `measurement_bits` | binary | packed Stim `b8` measurement row |
| `sweep_bits` | binary | packed sweep row, including an empty value if the experiment has no sweep bits |
| `detector_bits` | binary | packed Stim `b8` detector row |
| `detector_event_count` | int32 | number of set detector bits |
| `actual_observable_flip` | bool | actual logical outcome |
| `belief_matching_prediction` | bool | supplied decoder prediction |
| `correlated_matching_prediction` | bool | supplied decoder prediction |
| `pymatching_prediction` | bool | supplied decoder prediction |
| `tensor_network_contraction_prediction` | bool | supplied decoder prediction |

Keep measurements, detector events, actual outcomes, predictions, and decoder
mistakes as different concepts.

## 4. QASMBench circuits

Path: `silver/qasmbench/circuit.parquet`

One row represents one parsed circuit variant.

| Column | Type | Meaning |
| --- | --- | --- |
| `source_record_id` | string | stable link to the QASM member |
| `circuit_id` | string | stable circuit-variant identifier |
| `benchmark_name` | string | benchmark/circuit family |
| `variant` | string | source or transpiled variant |
| `register_declarations` | string | repeatable serialized register description |
| `qubit_count` | int32 | declared qubit count |
| `measurement_count` | int32 | executed measurement-operation count |
| `two_qubit_gate_count` | int32 | executed two-qubit-operation count |

If an operation addresses a register, count the expanded executed operations,
not only the number of QASM statement lines.

## 5. QASMBench stabilizer checks

Path: `silver/qasmbench/stabilizer_check.parquet`

One row represents one parity/stabilizer check identified in a circuit.

| Column | Type | Meaning |
| --- | --- | --- |
| `source_record_id` | string | stable link to the statements that define the check |
| `circuit_id` | string | link to the Silver circuit |
| `check_id` | string | stable identifier within the circuit |
| `ancilla_qubit` | string | ancilla/check qubit |
| `data_qubits` | list<string> | data qubits participating in the parity check |
| `syndrome_bit` | string | classical bit receiving the check result |

## 6. QASMBench conditional corrections

Path: `silver/qasmbench/conditional_correction.parquet`

One row represents one recovery operation controlled by a measured syndrome.

| Column | Type | Meaning |
| --- | --- | --- |
| `source_record_id` | string | stable link to the correction statement |
| `circuit_id` | string | link to the Silver circuit |
| `condition_register` | string | classical register used by the condition |
| `condition_value` | int64 | integer value that activates the operation |
| `gate` | string | correction gate name |
| `target_qubit` | string | qubit acted on by the correction |

## Minimum checks

For every table, test column names, types, row meaning, identifier stability,
and trace coverage. Reconcile rows read, rows accepted, and rows sent to
`results/part1/data_issues.parquet`. A second run on unchanged input must
produce the same records and identifiers.

These minimum tables are also the stable, course-managed data format that may
be provided to the separate visualization assignment. The visualization team
receives instructor-generated reference copies, not a particular student
team's output.
