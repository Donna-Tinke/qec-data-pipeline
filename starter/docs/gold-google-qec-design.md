# Gold design: Google surface-code source

This is the Gold (PostgreSQL) part of the Part I design report for the
`google_qec` source. It covers the data model, the reasons behind it, what it
costs, and how to reproduce it. The QASMBench and simulated-syndrome parts of
Gold are documented by their own authors.

- DDL, views and checks: `src/quantum_lake_student/gold/sql/google_qec/`
- Loader: `src/quantum_lake_student/gold/google_qec.py`
  (run with `python -m quantum_lake_student.gold.google_qec <run_id>`)
- Tests: `tests/test_gold_google_qec.py` (the database tests are skipped when
  PostgreSQL is unreachable)

## 1. What goes in: Silver → Gold

Silver gives two tables: `experiment` (5 rows) and `shot` (250,000 rows, one row
per aligned hardware shot, with packed bits, the actual flip, and four decoder
prediction columns). Gold does not copy them one-to-one. It reorganises them
by how they are used.

## 2. Tables (one sentence on what a row is)

| Table | One row represents | Key |
| --- | --- | --- |
| `gold.google_experiment` | one hardware experiment (distance, basis, rounds, processor location) | PK `experiment_id`; unique `source_record_id`; unique `(basis, distance, rounds, center_row, center_col)` |
| `gold.decoder` | one supplied decoder | PK `decoder_id`; unique `decoder_name` |
| `gold.google_shot` | one aligned shot: packed detector events + the actual logical flip | PK `(experiment_id, shot_index)`; unique `source_record_id`; FK → experiment |
| `gold.google_shot_raw` | the raw measurement and sweep bits of one shot (1:1 with `google_shot`) | PK = FK `(experiment_id, shot_index)` |
| `gold.google_shot_prediction` | what one decoder predicted for one shot | PK `(experiment_id, shot_index, decoder_id)`; FKs → shot, decoder |
| `gold.google_detector_position_stat` | how often one detector position fired in one experiment | PK `(experiment_id, detector_index)`; FK → experiment |

Views: `v_google_decoder_outcome` (shot × decoder with the derived mistake flag),
`v_google_shot_predictions_wide`, `v_google_example_lookup` (ML `example_id` →
shot), and `v_ml_google_decoder_example` (the ML table contract).

Constraints worth noting:

- `detector_event_count = bit_count(detector_bits)` is a `CHECK` on every shot,
  so the stored summary can never disagree with the stored bits.
- Experiments: `distance` odd and ≥ 3, `basis` in X/Z, positive counts.
- Post-load checks (`03_checks.sql`) cover what a row-level `CHECK` cannot:
  shots per experiment equal `shots`, four predictions per shot, a raw row per
  shot, `ceil(detector_count/8)` bytes per detector row, zero padding bits,
  and position statistics adding up to the event counts.

## 3. Design decisions and their costs

**Separate the four concepts.** The assignment says measurements, detector
events, actual outcomes, predictions and decoder mistakes are different
things. They are kept in different places: `google_shot_raw` (measurements),
`google_shot.detector_bits` (derived events), `google_shot.actual_observable_flip`
(label), `google_shot_prediction` (predictions). A decoder *mistake* is not
stored at all; it is `predicted_flip <> actual_observable_flip`, computed in a
view. Storing it would create a value that can disagree with its two inputs.

**Predictions in long form with a `decoder` table (rather than four columns).**
Adding or removing a decoder is a data change, not a schema change, and
per-decoder analyses are a plain `GROUP BY decoder_name`. *Cost:* 4 × 250,000 =
1,000,000 rows, 160 MB with indexes, far more than four boolean columns would
need, and the ML export has to pivot them (`v_google_shot_predictions_wide`).
We accept this because the data is small and because the same shape can later
hold predictions from our own decoders.

**Raw bits split from the hot shot table.** Every analysis and the ML export read
`google_shot`; only a few need the measurement bits (209–625 bits per shot).
Keeping them in `google_shot_raw` (45 MB) keeps `google_shot` narrow (92 MB).
*Cost:* one extra 1:1 table and join when the raw bits are needed.

**Detector storage strategy: packed bytes per shot + per-position summary.**
The source guide asks whether to store one row per fired detector or
per-position summaries. We measured it (`results/part1/analysis/google_detector_storage.json`,
produced by `measure_detector_storage`). The data has 10.4 million fired detector
events in total:

| Strategy | Size | Can reproduce the packed ML bytes? |
| --- | --- | --- |
| One row per fired detector (measured on the first 2,000 shots per experiment and extrapolated) | about 10.5 M rows, about 1.9 GB with its primary key | yes, but it must be re-packed |
| Packed `bytea` in each shot row (**chosen**) | 9 MB of bits (90 MB table with indexes and ids) | yes, it *is* the ML value |
| One byte per bit (what Silver contained before it was fixed) | 70 MB of bits | needs re-packing |
| Per-position summary (**chosen, in addition**) | 1,400 rows, 272 kB | no |

The packed column is the only copy of the events that the ML export needs
(`detector_bits` is exported unchanged, byte for byte). The per-position table
answers questions about *where* detectors fire without scanning 250,000 rows,
and is computed in SQL from the packed bits inside the load transaction and
reconciled against `detector_event_count`. A long event table would be about
200× larger than the packed column and was not needed by any required analysis.
*Cost:* asking "which shots fired detector 17?" means `get_bit(detector_bits, 17)`
over a scan, not an index lookup. That is acceptable at this scale.

**Bit order.** Stim `b8` is little-endian inside each byte. PostgreSQL's
`get_bit(bytea, n)` uses the same numbering (bit `n` is bit `n % 8` of byte
`n / 8`, counted from the least significant bit), which is covered by
`test_position_stats_use_b8_bit_order`.

**Natural keys.** `experiment_id` is the source directory name and
`(experiment_id, shot_index)` is the shot key. Both come from the source, so
reruns give the same keys and there are no surrogate sequences that could change
between loads. `decoder_id` is a small fixed smallint.

**Indexes.** The primary keys already serve the main access paths (all shots of
an experiment, one shot, all predictions of a shot). Extra indexes are only
those with a reason: `google_shot_prediction (decoder_id, experiment_id)` for
per-decoder scans and for the foreign key, `google_experiment (distance)` for
the by-distance filter, and the unique `source_record_id` columns for tracing
from Silver. We did not add speculative indexes on boolean columns.

## 4. All-or-nothing load

`load_gold` runs the DDL, `COPY` of both Silver tables, the inserts, the derived
position table, the views and `03_checks.sql` in **one transaction**. PostgreSQL
DDL is transactional, so any failure (a failed check, a constraint, a crash)
rolls everything back and the previous complete version stays visible.
`tests/test_gold_google_qec.py` loads a good version, then attempts bad loads
(wrong event count, set padding bit, wrong byte length, wrong shot count) and
asserts that the previous version is still there. The load drops and
recreates only the objects named `google_*`, so other sources sharing the
`gold` schema are untouched. A rerun on unchanged input yields identical row
counts and identical `example_id`s (tested).

## 5. Tracing

- Every `google_experiment` and `google_shot` row keeps the Silver
  `source_record_id`, which `results/part1/source_trace.parquet` maps to the
  Bronze archive, member files (several per shot) and hash.
- `example_id = sha256('google_qec:' || experiment_id || ':' || shot_index)`.
  It depends only on source keys, so it is stable across reruns, and
  `v_google_example_lookup` resolves it back to the Gold shot and its
  `source_record_id`.

## 6. Rejected relationship: Google experiments ↔ QASMBench circuits

QASMBench circuits (`qec_sm_n5` etc.) and the Google experiments both describe
parity checks, so one might link them. We did not. The Google directories
carry a code distance, basis, rounds and processor location; the QASM circuits
carry register names and qubit counts. Neither has a field in common (not an
id, a qubit layout, nor a device), and the QASM circuits are 5-qubit
textbook examples whereas the Google experiments have 17–49 qubit surface
codes. Any join would be a guess, so Gold keeps them as separate entities with
no foreign key; they share only vocabulary (distance, rounds, check).

## 7. Analyses (reproducible SQL, written to `results/part1/analysis/`)

| File | Question |
| --- | --- |
| `google_decoder_error_rates.sql` | Decoder logical-error rate by distance and processor location (joins experiment, shot, prediction and decoder: four tables) |
| `google_decoder_error_overlap.sql` | How many of the four decoders are wrong on the same shot? |
| `google_detector_hotspots.sql` | Which detector positions fire most per experiment? |

Observations from the loaded data (associations only): the four decoders'
mistake rates differ by only a few percentage points per experiment, they are
all below the actual flip rate (about 49% versus roughly 40–43% for distance 3,
so the decoders help only modestly over 25 noisy rounds), and the number of decoders
wrong on the same shot is spread across 0–4 (about 30% of distance-3 shots have
no decoder wrong, about 14% have all four wrong), so the mistakes only partly
overlap, which is what a combined decoder can use.

## 8. Known limits

- `gold.google_shot` stores `detector_event_count`, which is redundant with the
  bits; the `CHECK` constraint is what keeps it honest.
- Position statistics are a derived table; they are rebuilt on every load, not
  incrementally.
- Data-quality findings (rejected rows) are handled in Silver; Gold only receives
  accepted records.
- During this work a defect in the Silver stage was found and fixed: the shot
  bit columns were stored one byte per bit instead of as packed `b8`
  (`pack_bits` in `sources/google_qec.py`).
