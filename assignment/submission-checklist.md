# Submission checklist

## Repository and reproduction

- [ ] Course archives, credentials, generated volumes, and large temporary
      outputs are not committed.
- [ ] Python/dependency versions and random seeds are fixed.
- [ ] One documented command runs Part I, one runs Part II, and one runs tests.
- [ ] Part I succeeds twice without duplicate business records or changed
      stable identifiers.
- [ ] All three required data sources are processed.

## Architecture and required outputs

- [ ] The course architecture is used, or a short motivation for the chosen
      alternative is included.
- [ ] Bronze is unchanged and read-only.
- [ ] All six tables in [silver-tables.md](silver-tables.md) exist and match
      their row meanings, columns, and types.
- [ ] Gold is a student-designed PostgreSQL model with keys, constraints,
      indexes, and an all-or-nothing load.
- [ ] Both tables in [required-ml-tables.md](required-ml-tables.md) exist.
- [ ] Both ML tables are produced through committed Gold queries/views plus
      export code; the export does not re-read Bronze or Silver.
- [ ] All required files under `results/part1/` and `results/part2/` exist.

## Part I: discovery, cleaning, and quality

- [ ] Bronze hashes and safe archive-member paths are verified.
- [ ] Syndrome shape, bit domain, labels, positive weights, and weighted totals
      reconcile.
- [ ] Stim `b8` length, byte alignment, little-endian bit order, and unused
      padding are tested.
- [ ] Every Stim `01` file aligns to the declared shots and contains only
      binary values.
- [ ] Measurements, detector events, actual flips, predictions, and decoder
      mistakes are kept as different concepts.
- [ ] Valid same-syndrome/different-label rows remain represented.
- [ ] QASM operations, parity checks, measurements, and conditional corrections
      are parsed and checked.
- [ ] `results/part1/data_issues.parquet` preserves each invalid value, rule,
      severity, action, and reason.
- [ ] `run.json` records hashes, code revision, times, and per-table counts.
- [ ] `row_counts.json` reconciles read, accepted, rejected, and loaded rows.

## Part I: Gold, analysis, and tracing

- [ ] Every Gold relation states what one row represents.
- [ ] Shared QEC vocabulary is used without inventing cross-source identifiers.
- [ ] Experiments, syndromes, shots, detector summaries, decoder outputs,
      circuits, stabilizer checks, and corrections are queryable.
- [ ] The chosen detector-storage approach is measured and explained.
- [ ] The Part I design report covers source discovery, architecture, Gold
      design, important decisions, quality findings, and one rejected relation.
- [ ] All three analysis questions use reproducible SQL against Gold, including
      one query that joins at least three Gold tables.
- [ ] Every Silver row has a stable `source_record_id` represented in
      `source_trace.parquet`.
- [ ] Every ML `example_id` resolves to relevant Gold record(s), and predictions
      retain `example_id`.
- [ ] `trace_examples.json` demonstrates one complete syndrome trace and one
      complete Google trace.

## Part II: syndrome task

- [ ] The weighted majority/prior and one weighted linear classifier run.
- [ ] Supplied partitions are used without moving examples between them.
- [ ] Physical weights are used in all Task A evaluation metrics and in fitting
      where supported.
- [ ] Logical-error rate, balanced accuracy, Brier score, training time, and
      prediction time are reported where applicable.

## Part II: Google tasks

- [ ] Distance-three and distance-five decoder comparisons are separate.
- [ ] All four supplied decoders are compared on the same held-out shots.
- [ ] The linear combined decoder uses event density and the four aligned
      predictions.
- [ ] The raw-detector MLP uses only distance-three rows with
      `shot_index < 12_500` and the supplied split.
- [ ] The short report explains decoder-error overlap and what a flat raw-bit
      input does not show explicitly.

## Part II: results and tests

- [ ] One command creates fitted models, `predictions.parquet`, `metrics.json`,
      `run.json`, and `report.md`.
- [ ] Release version, both ML-table hashes, code revision, dependencies,
      feature order, split, seed, and timings are recorded.
- [ ] Tests cover required columns/types, split use, bit order, event counts,
      weighting, repeatability, and metric calculations.
- [ ] Performance claims use identical examples and state limitations.
- [ ] The report does not hide a weak or negative model result; model
      performance itself is not part of the grade.

## Demonstration

- [ ] Trace one syndrome prediction through ML, Gold, Silver, and Bronze.
- [ ] Trace one Google prediction to its shot and aligned Bronze members.
- [ ] Reproduce Part I analyses and Part II metrics from a clean run.
- [ ] Every team member can explain one data decision and one model decision.
