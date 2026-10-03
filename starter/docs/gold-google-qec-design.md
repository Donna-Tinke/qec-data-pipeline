# Gold design: Google surface-code source

How the `google_qec` data is stored in PostgreSQL (Gold), and why.

- SQL (tables, views, checks): `src/quantum_lake_student/gold/sql/google_qec/`
- Loader: `python -m quantum_lake_student.gold.google_qec <run_id>`
- Tests: `tests/test_gold_google_qec.py` (DB tests skip if PostgreSQL isn't running)

## Tables

Silver has 5 experiments and 250,000 shots. Gold splits them up by how they get used:

| Table | One row is... |
| --- | --- |
| `google_experiment` | one hardware experiment (distance, basis, rounds, location) |
| `decoder` | one decoder |
| `google_shot` | one shot: packed detector events + the actual logical flip |
| `google_shot_raw` | the raw measurement and sweep bits of one shot |
| `google_shot_prediction` | one decoder's prediction for one shot |
| `google_detector_position_stat` | how often one detector fires in one experiment |

There are also a few views, including the one the ML export reads
(`v_ml_google_decoder_example`).

## Choices

**Mistakes aren't stored.** A decoder mistake is just `predicted_flip <> actual_observable_flip`,
so a view works it out. Storing it could let it disagree with the two columns it comes from.

**Predictions are one row per decoder per shot.** Adding a decoder is then new data,
not a schema change. It costs 1M rows (160 MB) and a pivot for the ML export, but
the data is small so that's fine.

**Raw bits live in their own table.** Most queries only need `google_shot`, so I kept
the big measurement bits out of it.

**Detector events are stored packed, one `bytea` per shot, plus a small per-position
table.** I measured the alternative of one row per fired detector: about 10.5M rows
and 1.9 GB, roughly 200x bigger than the packed column. The packed bytes are also
exactly what the ML export needs. The downside is that finding shots where one
detector fired means scanning with `get_bit`, which is fine at this size.

**Bit order.** Stim `b8` and PostgreSQL `get_bit` number bits the same way, so the
position stats work straight off the packed bytes (tested).

**Keys come from the source** (`experiment_id`, `shot_index`), so reruns give the
same keys.

## Loading

Everything (DDL, `COPY`, inserts, views, checks) runs in one transaction. If any
check fails, it all rolls back and the previous version stays. The tests try bad
loads (wrong event count, padding bits set, wrong byte length, wrong shot count) to
prove this. Only `google_*` objects are touched, so other sources in `gold` are safe.

## Tracing

Each experiment and shot keeps its Silver `source_record_id`, which
`results/part1/source_trace.parquet` links back to the Bronze archive.
`example_id` is a hash of the experiment id and shot index, so it's stable across
reruns and `v_google_example_lookup` maps it back to the shot.

## Not linked to QASMBench

I didn't link the Google experiments to the QASMBench circuits. They share no field
(id, qubit layout, device), and the QASM ones are 5-qubit textbook examples while
Google's are 17-49 qubit surface codes. Any join would be a guess.

## Analyses

SQL files are in `gold/sql/google_qec/analysis/`:

- `google_decoder_error_rates.sql`: error rate per decoder by distance and location
- `google_decoder_error_overlap.sql`: how many decoders are wrong on the same shot
- `google_detector_hotspots.sql`: which detectors fire most

Roughly what they show: decoders are only a few points apart and beat the raw flip
rate by a modest margin (about 40-43% vs 49% at distance 3). Their mistakes only
partly overlap, which a combined decoder could use.

## Limits

- `detector_event_count` duplicates info in the bits; a `CHECK` keeps it honest.
- Position stats are rebuilt on every load, not incrementally.
- Bad records are rejected in Silver, so Gold only sees accepted ones.
- While doing this I found Silver stored bits one byte per bit instead of packed
  `b8`. Fixed with `pack_bits` in `sources/google_qec.py`.
