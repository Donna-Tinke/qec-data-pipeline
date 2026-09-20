# DSAIT4000 Project 1: QEC data pipeline for AI

## Review specification — version 6

**Format:** teams of 2–3  
**Schedule:** four weeks total  
**Scope balance:** Part I Data Management 60%; Part II AI/ML consumer 40%  
**Environment:** local Docker platform; no external credentials or quantum
hardware access

## 1. Purpose and grading principle

Students build a repeatable data pipeline for three quantum error-correction
(QEC) sources. Part I checks, cleans, connects, and stores the data. Part II
demonstrates that the resulting tables can feed small ML experiments.

The assignment is about data management and the hand-off to ML. **Numerical
model performance is not part of the grade.** Students do not need to
outperform supplied decoders or reach a target score. ML marks reward correct
prepared-data use, meaningful baselines, repeatability, evaluation, testing,
and honest interpretation.

## 2. Course architecture and allowed deviations

```text
bronze  unchanged supplied objects
  -> silver  six minimum source-specific Parquet tables
  -> gold    student-designed PostgreSQL QEC model
  -> ml      two fixed Parquet tables for Part II

results/part1  issues, run records, counts, analyses, traces
results/part2  predictions, metrics, run record, short report
```

This is described to students as the course-specific bronze–silver–gold
architecture with an explicit ML hand-off. It gives beginners a concrete
structure and connects data collection, discovery, cleaning, RDBMS modeling,
integration, indexing, data lakes, and databases for ML.

Students may use a different internal architecture when they give a short,
clear motivation. Every solution must still produce the required compatibility
outputs: unchanged Bronze, the six standardized Silver tables, PostgreSQL
Gold, both exact ML tables, the results layout, and end-to-end tracing. A
deviation earns no extra credit and cannot bypass a required stage.

## 3. Learning outcomes

Students should be able to:

1. keep scientific source files unchanged and trace results to them;
2. inspect unfamiliar formats and state what each file and row represents;
3. find identifiers, one-to-one and one-to-many relations, and rejected joins;
4. clean CSV, OpenQASM, YAML, Stim `b8`, and Stim `01` data;
5. publish tested, source-specific Parquet tables;
6. design a relational QEC model with keys, constraints, and indexes;
7. export fixed ML tables through student-written queries over that model;
8. train and evaluate small, repeatable downstream consumers; and
9. reproduce outputs and trace predictions to source records.

## 4. Supplied three-source release

| Source | Supplied content | Main Data Management use | ML use |
| --- | --- | --- | --- |
| Simulated syndromes | Seven d=3 CSVs; 75,598 aggregate rows; 70m weighted observations | nested 4-by-4 values, filename values, weights, non-unique syndrome values | weighted prior and linear classifier |
| Google hardware QEC | Four d=3 and one d=5 experiment; 250k shots | packed bits, companion files, shot alignment, decoder predictions | supplied-decoder comparison, linear combined decoder, bounded raw-bit MLP |
| QASMBench QEC circuits | Three directories; source and transpiled QASM | registers, parity checks, syndrome bits, recovery rules | no labeled training rows |

Dataset attribution and context remain in `data-sources.md`. Attribution is
source documentation, not a student reading or citation task.

The student data ZIP contains exactly these objects. It is 15,143,813 bytes
with SHA-256
`21f27141368789e0c0fb733a4853c085dd9490fddb80462abf0f3e18d801ee86`.

## 5. Part I specification

### Bronze

Students verify object hashes and archive paths, preserve original bytes, and
never correct data in place.

### Silver

The course now specifies six minimum Parquet tables:

1. `qec_syndromes/syndrome_observation`;
2. `google_qec/experiment`;
3. `google_qec/shot`;
4. `qasmbench/circuit`;
5. `qasmbench/stabilizer_check`; and
6. `qasmbench/conditional_correction`.

The exact row meanings, minimum columns, and types are in `silver-tables.md`.
Teams may add fields/tables but may not integrate across sources in Silver.
This standardization reduces ambiguity and provides a stable reference format
for the separate visualization assignment.

Minimum cleaning work includes:

- syndrome header, 4-by-4 shape, bit domain, positive weights, and weighted totals;
- valid same-syndrome/different-label cases;
- `b8` length, alignment, little-endian bit order, and padding;
- `01` row/domain checks and actual/prediction alignment;
- separation of measurements, detector events, outcomes, predictions, and errors;
- QASM register identity, executed operations, parity checks, and corrections;
- required companion files and safe archive member paths.

Invalid records are excluded from Silver and preserved in
`results/part1/data_issues.parquet`.

### Gold

Students design PostgreSQL Gold. Required concepts include experiments,
syndromes, shots, detector summaries, decoders/predictions, circuits,
stabilizer checks, and corrections. Tables require clear row meanings, keys,
constraints, useful indexes, and an all-or-nothing load. Teams must preserve
the evidence that no row-level QASMBench-to-experiment join exists.

Students choose and measure a detector-storage strategy. They are not required
to produce one database row per detector bit.

### Gold-to-ML export

Students write the joins, stable ML IDs, SQL queries/views, and export code.
Every identifier, feature, label, and weight comes from Gold. A thin Python
step may run SQL, call supplied split helpers, check contracts, and write
Parquet. The course supplies contracts, split helpers, packed-bit readers, and
tests—not completed queries or export templates.

### Results and run evidence

```text
results/part1/
  data_issues.parquet
  run.json
  row_counts.json
  source_trace.parquet
  trace_examples.json
  analysis/
```

`run.json` records input hashes, code revision, start/end times, and every
output count. `data_issues.parquet` has stable issue/run/source IDs, rule,
severity, observed value, action, and reason.

### Tracing contract

- every Silver row has `source_record_id`;
- `source_trace.parquet` maps it to Bronze object/member/record/hash;
- Gold records used by ML resolve to Silver records;
- each ML `example_id` resolves to relevant Gold record(s); and
- each prediction retains `example_id`.

Students demonstrate one full syndrome trace and one full Google trace. Long
source strings need not be copied into every table.

### Required analyses

Students answer the same three questions about weighted syndrome behavior,
Google decoder error rates by distance/location, and QASM parity/correction
mapping. They use Gold SQL, including one join across at least three tables.

## 6. Part II specification

Part II reads only the two fixed ML Parquet tables.

### Task A: weighted syndrome model

- weighted majority/prior baseline;
- one weighted linear classifier;
- supplied fault-rate partitions; and
- physical weights for all evaluation and fitting where supported.

The previous syndrome MLP requirement is removed.

### Task B: Google decoder comparison and combination

For distance three and five separately:

- majority/prior baseline;
- all four supplied decoders evaluated on identical test shots; and
- one simple linear combined decoder using event density and four predictions.

The previous nonlinear combined decoder is removed.

### Task C: bounded raw-detector consumer

One small MLP consumes the 200 unpacked detector bits for the fixed subset
`distance = 3 AND shot_index < 12_500` across the four distance-three
experiments. Students record timing and explain in their own words what the
flat representation hides. No complex architecture proposal or external model
research is required.

### Metrics and outputs

Required metrics are logical-error rate, balanced accuracy, Brier score when a
probability exists, training time, and prediction time. ROC AUC and log loss
are removed from the student requirement.

```text
results/part2/
  predictions.parquet
  metrics.json
  run.json
  report.md
```

One command also creates fitted model files. Tests cover input contracts,
splits, physical weights, bit order/event counts, repeatability, and metrics.
There is one concise report rather than separate model cards.

## 7. Four-week cadence and effort estimate

| Time | Work | Approximate team effort |
| --- | --- | ---: |
| Week 1 | setup, Bronze/discovery, syndrome and QASM Silver | 10–12 h |
| Week 2 | Google Silver, quality evidence, Gold design/load | 11–13 h |
| First half week 3 | Gold completion, ML exports, analyses, tracing, rerun | 7–9 h |
| Second half week 3 | Task A and Part II results framework | 5–6 h |
| Week 4 | Tasks B/C, evaluation, tests, clean-run review | 11–15 h |

Estimated total: **44–55 team hours**, with **28–34 hours in Part I** and
**16–21 hours in Part II**. Estimates assume the supplied platform, readers,
helpers, and starter tests work as documented.

## 8. Assessment design

| Area | Weight |
| --- | ---: |
| Part I import/source information/safe reruns | 11% |
| Part I cleaning/quality | 14% |
| Part I discovery/modeling | 13% |
| Part I Parquet/PostgreSQL/ML input tables | 15% |
| Part I operation/tests/analysis | 7% |
| Part II weighted syndrome model | 12% |
| Part II Google combined decoder | 12% |
| Part II raw-detector prototype | 6% |
| Part II repeatability/evaluation/tests | 10% |

Missing Part I caps the grade at 40; missing Part II caps it at 60. Other caps
cover missing compatibility outputs, unsafe repeated runs, silent data changes,
ML tables that bypass Gold, training from source archives, ignored weights, and
confused QEC concepts.

## 9. Documentation burden

Students submit one concise Part I design report covering discovery,
architecture, Gold design, important decisions, the rejected relationship,
quality findings, and trace examples. Machine-readable issues, counts, run
records, and Part II results remain separate. Part II has one short report.

There is **no student external-reading, source-count, citation, or
research-based model-justification requirement**. Dataset DOI/source
attribution remains provided by the course.

## 10. Visualization hand-off

The visualization teaching team should receive instructor-generated reference
copies of the six standardized Silver tables, plus separate example ML tables
if their assignment uses model-ready data. They should not depend on outputs
from an individual student submission. `silver-tables.md` is the format
contract to share with that team.
