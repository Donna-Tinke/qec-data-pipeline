# Required ML input tables

Part I creates these two Parquet tables in the `ml` zone and Part II reads
them. Their columns are prescribed so every team performs the same model tasks.
Use the supplied helpers for split assignment and model inputs.

Build the content of both tables with committed SQL queries or views against
your Gold schema. Write the joins, ID construction, and export logic
yourselves. A small Python step may run the SQL, call the supplied split
functions, check the result, and write Parquet. It may not re-read Bronze or
Silver. If a required value is hard to produce from Gold, fix the Gold model.


## `ml_syndrome_decoder_example`

One row represents one distinct syndrome-and-label observation within one
physical-fault-rate experiment.

| Column | Type | Meaning |
| --- | --- | --- |
| `example_id` | string | Repeatable hash of experiment, syndrome, and label |
| `experiment_id` | string | Repeatable simulated-experiment identifier |
| `physical_fault_rate` | float64 | Fault rate encoded by the source filename |
| `syndrome_bits` | binary | Exactly 16 binary values, ordered by round and then check |
| `round_count` | int32 | Must be 4 |
| `check_count` | int32 | Must be 4 |
| `logical_error_label` | bool | Value the model must predict |
| `sample_weight` | int64 | Source `quantity`: number of shots represented by the row |
| `data_split` | string | `train`, `validation`, or `test` |

Each `example_id` must resolve to the relevant Gold record or records through
a relation or view designed by the team.

Use these splits:

- validation: physical fault rate `0.0005`;
- test: physical fault rate `0.005`;
- train: the other five fault-rate files.

The starter function `syndrome_data_split` returns these values from the
physical fault rate. This makes every team use the same comparison.

## `ml_google_decoder_example`

One row represents one hardware shot in one Google surface-code experiment.

| Column | Type | Meaning |
| --- | --- | --- |
| `example_id` | string | Repeatable experiment-and-shot hash |
| `experiment_id` | string | Repeatable experiment identifier |
| `shot_index` | int64 | Source shot number, starting at zero |
| `distance`, `rounds` | int32 | Code parameters; rounds are 25 in this release |
| `center_row`, `center_col` | int32 | Processor-location coordinates |
| `detector_count` | int32 | 200 for distance 3; 600 for distance 5 |
| `detector_event_count` | int32 | Number of fired detector bits in the shot |
| `detector_bits` | binary | Stim `b8` row; bits are little-endian inside each byte |
| `*_prediction` | bool | Prediction from one of four supplied decoders |
| `actual_observable_flip` | bool | Value the model must predict |
| `data_split` | string | Main shot-based split |

Each `example_id` must resolve to the relevant Gold shot record. The source
trace for that shot may contain several Bronze companion files.

The four prediction columns are:

- `belief_matching_prediction`;
- `correlated_matching_prediction`;
- `pymatching_prediction`;
- `tensor_network_contraction_prediction`.

Use this main split within every experiment:

- test: odd `shot_index` values;
- validation: even values for which `shot_index % 10 == 8`;
- train: all remaining even values.

This gives 40% train, 10% validation, and 50% test data. All teams use the same
split so results can be checked consistently.

The starter function `google_data_split` returns these values from
`shot_index`.

## Checks your code must perform

- example IDs are unique in each ML input table;
- train, validation, and test are not empty;
- every syndrome has exactly 16 binary values;
- every packed Google row contains `ceil(detector_count / 8)` bytes and unused
  padding bits are zero;
- `detector_event_count` equals the number of set detector bits;
- labels and decoder outputs are binary and aligned by experiment and shot;
- distance-three and distance-five raw-detector matrices are represented
  separately because their feature widths differ;
- the supplied model-input helpers return the documented inputs and target;
- both tables were produced from Gold queries/views and the allowed export
  step, not by re-reading Bronze or Silver;
- every `example_id` resolves to the relevant Gold record or records.

## Required filenames and run information

Expose these tables to the training command as
`ml/ml_syndrome_decoder_example.parquet` and
`ml/ml_google_decoder_example.parquet`. Training must record the hashes of both
input tables and the course data-release identifier.

The course supplies these contracts, split helpers, low-level packed-bit
readers, and contract tests. It does not supply completed Gold-to-ML queries or
feature-export templates.
