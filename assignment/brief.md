# Assignment brief: QEC data pipeline for AI

## Scenario and scope

A quantum error-correction (QEC) research group has circuit definitions,
simulated syndrome sequences, hardware measurements, detector events, logical
outcomes, and decoder predictions in several file formats. Your team will build
one repeatable system that turns these source files into checked data, a
relational database, and two tables that can be used for machine learning.

The assignment lasts **four weeks**:

1. **Part I — Data Management (about 60% of the work):** inspect, clean,
   connect, store, and document the data;
2. **Part II — AI/ML (about 40% of the work):** show that the prepared data can
   be consumed by a small, repeatable model pipeline.

All three supplied sources and all tasks below are required. The ML models are
downstream consumers of the data pipeline; this is not a model-performance
competition.

## Learning objectives

By completing the assignment, you should be able to:

1. keep supplied files unchanged and trace later results back to them;
2. inspect unfamiliar data and explain what each file and row represents;
3. identify useful identifiers and one-to-one or one-to-many relationships;
4. find data-quality problems and record how each problem was handled;
5. create cleaned Parquet tables and a student-designed PostgreSQL model;
6. create two fixed ML input tables from PostgreSQL;
7. train and evaluate small models using the supplied data partitions; and
8. reproduce a result and trace it back to the supplied source data.

The [plain-language guide](plain-language-guide.md) explains the specialist
terms used in this brief. The [quantum-data primer](quantum-data-primer.md)
explains the QEC data; no prior quantum-computing course is required.

## Required data

Use exactly these three supplied datasets:

- `qec_syndromes`: seven simulated distance-three surface-code CSV files with
  aggregate syndrome sequences, logical-error labels, and row weights;
- `google_qec`: five 25-round hardware experiments with measurements, detector
  events, actual logical flips, and predictions from four existing decoders;
- `qasmbench`: three QEC circuit directories with OpenQASM source and
  transpiled circuits, including parity measurements and conditional recovery.

QASMBench cannot be joined row by row to the Google or simulated experiments:
the sources do not share an experiment identifier. Store them in the same QEC
database, but do not invent a match.

# Part I — Data Management

## The course architecture

The recommended design has four named areas:

```text
bronze/source=<source>/     supplied files, unchanged
      |
      | parse + check              issues and run records -> results/part1/
      v
silver/<source>/<table>/    cleaned, source-specific Parquet tables
      |
      | connect + model + load
      v
gold (PostgreSQL)           your relational QEC model and analysis queries
      |
      | student-written queries and export code
      v
ml/                         two fixed Parquet tables for Part II
```

This is the course-specific form of a bronze–silver–gold architecture. The
extra `ml` area makes the hand-off to Part II explicit.

| Area | Plain-language meaning | Who decides its shape? |
| --- | --- | --- |
| `bronze` | the supplied bytes | the course |
| `silver` | checked data, still separated by source | the course defines the minimum tables |
| `gold` | a connected PostgreSQL model | your team |
| `ml` | two fixed training tables | the course |
| `results` | issues, run facts, analyses, predictions, and metrics | required layout below |

You may use a different internal architecture if your team gives a short,
clear reason. A different design must still produce all required compatibility
outputs: unchanged `bronze`, the minimum `silver` tables, the PostgreSQL
`gold` model, the two exact `ml` tables, the `results` files, and the required
source tracing. A deviation earns no extra marks and may not skip a required
step.

### Bronze: supplied and read-only

The platform places the three source objects under `bronze/source=<source>/`.
Do not rename, reformat, or fix them in place. Verify their hashes and archive
member paths before reading them. Corrections happen while building `silver`.

### Silver: checked, source-specific Parquet

Silver tables remain faithful to the source. A syndrome CSV row remains one
aggregate observation; a Google shot remains one shot. Values are parsed into
clear types and must pass your checks before entering Silver. Do not join the
three data sources here. If the format is new to you, read
[Parquet in this assignment](plain-language-guide.md#parquet-in-this-assignment) before implementing Silver.

Build the six minimum tables defined in [silver-tables.md](silver-tables.md):

- one syndrome observation table;
- a Google experiment table and a Google shot table; and
- a QASMBench circuit table, stabilizer-check table, and correction table.

You may add useful Silver tables or columns. Every Silver row has a stable
`source_record_id`. Invalid records are excluded from Silver and recorded in
`results/part1/data_issues.parquet` with the original value and the reason.

### Gold: your PostgreSQL model

Gold is the main student-designed part of the assignment. Decide what entities
and relationships are useful, what one row represents, and which keys,
constraints, and indexes PostgreSQL should enforce.

At minimum, the database must support questions about experiments, syndrome
patterns and values, shots, detector summaries, decoders and predictions,
circuits, stabilizer checks, and conditional corrections. Record important
design decisions and one investigated but rejected relationship. Load Gold as
an all-or-nothing update: either the complete load succeeds, or the previous
complete version remains available.

### ML: fixed tables exported from Gold

ML contains the two Parquet tables in
[required-ml-tables.md](required-ml-tables.md). All identifiers, features,
labels, weights, and source values must come from committed SQL queries or
views over your Gold model. Write the joins, ID construction, queries, and
export logic yourselves.

The required files are `ml/ml_syndrome_decoder_example.parquet` and
`ml/ml_google_decoder_example.parquet`.

A small Python export step may run the SQL, call the supplied split functions,
validate the result, and write Parquet. Do not re-read Bronze or Silver. The
course supplies the table schemas, split helpers, packed-bit helpers, and
contract tests; it does not supply completed Gold-to-ML queries.

## Required results layout

Keep generated evidence outside the four data areas:

```text
results/
  part1/
    data_issues.parquet
    run.json
    row_counts.json
    source_trace.parquet
    trace_examples.json
    analysis/
  part2/
    predictions.parquet
    metrics.json
    run.json
    report.md
```

In other words, data-management evidence goes under `results/part1/` and model
evidence goes under `results/part2/`.

`data_issues.parquet` must include at least `issue_id`, `run_id`, nullable
`source_record_id`, `rule_id`, `severity`, `observed_value`, `action`, and
`reason`. `run.json` for Part I records input hashes, code revision, start and
end time, and the row count of every output table. `row_counts.json` makes the
Bronze-to-Silver and Silver-to-Gold count checks easy to review.

## Source tracing

Tracing uses stable identifiers instead of copying long source paths into every
table:

1. every Silver row has a `source_record_id`;
2. `results/part1/source_trace.parquet` maps each `source_record_id` to the
   Bronze object, archive member, record position, and input hash;
3. Gold records used by ML can be resolved to their Silver records;
4. every ML `example_id` can be resolved to the relevant Gold record or
   records through a relation or view you design; and
5. every Part II prediction contains the matching `example_id`.

One Google shot may need several trace rows because its aligned files are
separate Bronze members. Demonstrate the complete trace for one syndrome
prediction and one Google prediction in `trace_examples.json`.

## Data discovery and cleaning

For each source, document its files and formats, what one row or binary record
represents, candidate identifiers, relationships, packed or nested values, and
which companion files belong together. Investigate at least one possible
relationship that you reject and preserve the evidence for that decision.

At minimum, implement and test checks for:

- documented `label` versus actual `labels` in the syndrome CSV header;
- four rounds with four binary syndrome values per round;
- positive `quantity` values and ten million weighted observations per fault-rate file;
- the valid case where one syndrome occurs with both labels;
- Stim `b8` length, byte alignment, bit order, and unused padding bits;
- Stim `01` row count and binary values;
- alignment of actual and predicted logical flips;
- the difference between measurements, detector events, labels, and decoder errors;
- QASM registers, executed operations, parity checks, measurements, and
  syndrome-controlled recovery; and
- required companion files and safe archive member names.

Record the outcome of every check. Expected observations and warnings may
remain in Silver. Invalid records go to the data-issues table; a missing
required companion file or unsafe archive member must stop the run.

## Gold design requirements

For every Gold table, state in one sentence what one row represents. Use
primary keys, uniqueness rules, foreign keys, value checks, and useful indexes.
Do not merely copy each Silver table into PostgreSQL.

The sources should use consistent names for shared ideas, but shared vocabulary
does not create a shared identifier. Keep QASMBench circuits as their own
entities because no row-level match to the experiments is supplied.

You do not have to store every detector bit as a separate database row. A long
event table or a shot table with summaries can both be reasonable. Measure the
size, explain the choice, and prove that the ML export can reproduce the packed
bytes without returning to Bronze or Silver.

Repeated runs on unchanged inputs must leave stable identifiers and must not
add duplicate business records.

## Part I analysis questions

Answer all three with reproducible SQL against Gold:

1. How do weighted syndrome frequency and logical-error labels change with
   physical fault rate?
2. How do the supplied decoder logical-error rates compare by code distance and
   distance-three processor location?
3. How does the repetition-code circuit map data qubits to parity-check
   ancillas, syndrome bits, and conditional corrections?

At least one query must join three or more Gold tables. Describe associations
in the data; do not claim that one variable causes another unless the data
supports that conclusion.

# Part II — AI/ML consumer

> **Model performance is not part of the grade.** You are not graded on
> outperforming the supplied decoders or reaching a particular score. Marks
> are based on correct use of prepared data, meaningful baselines,
> repeatability, evaluation, testing, and honest interpretation.

Complete the three bounded tasks in [part-2-ai-ml.md](part-2-ai-ml.md):

1. a weighted prior and one weighted linear syndrome decoder;
2. comparison of the four supplied Google decoders and one simple linear
   combined decoder, separately for distance three and distance five; and
3. one small distance-three MLP using raw detector bits on a fixed subset.

Report logical-error rate, balanced accuracy, Brier score, training time, and
prediction time. Use the supplied partitions. A model that performs worse than
a baseline is a complete result when the experiment is correct and the result
is explained honestly.

## Suggested four-week schedule

| Time | Main work |
| --- | --- |
| Week 1 | setup, Bronze inventory, discovery, syndrome Silver, QASMBench Silver |
| Week 2 | Google Silver, quality evidence, Gold design and first load |
| First half of week 3 | finish Gold, export both ML tables, SQL analyses, tracing, rerun test |
| Second half of week 3 | Part II Task A and saved-results structure |
| Week 4 | Tasks B and C, evaluation, tests, clean-run check, submission review |

Estimated team effort is **28–34 hours for Part I** and **16–21 hours for Part
II** (about **44–55 hours total**). These estimates assume you start from the
supplied platform, helpers, and starter tests.

## Complete deliverables

Submit:

1. runnable Part I and Part II code with fixed dependency versions;
2. one documented command for Part I, one for Part II, and one for tests;
3. the six minimum Silver tables, both ML tables, and the required `results/`
   files generated by your code;
4. Gold table definitions, constraints, indexes, all-or-nothing load process,
   and the committed SQL/views used by the ML export;
5. automated tests for cleaning, repeated runs, contracts, bit order,
   weighting, splits, tracing, and metrics;
6. one concise Part I design report covering source discovery, the architecture
   used, Gold design, important decisions, the rejected relationship, quality
   findings, and example traces;
7. the three SQL analyses with short interpretations;
8. fitted model files, prediction Parquet, metrics, run settings, and the short
   Part II report; and
9. evidence that everything runs from a clean environment.

Do not commit course archives, credentials, generated database volumes, or
large temporary model files to the team repository.

## Definition of done

From a clean environment, the teaching team can run Part I twice without
duplicates, inspect the Silver and Gold data, rebuild both ML tables, run Part
II with one command, reproduce the reported metrics using the supplied
partitions, trace a syndrome and a Google prediction back to Bronze, and run
all automated tests.

The grading focus is data management and a reliable hand-off to ML. **The
numerical performance of the models is not graded.**
