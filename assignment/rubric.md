# Assessment rubric

Total: 100 points. Part I contributes 60 points and Part II contributes 40
points.

**Model performance is not graded.** There is no required score and teams do
not have to outperform the supplied decoders. Part II marks reward correct data
use, appropriate comparisons, repeatability, evaluation, testing, and honest
interpretation.

The course architecture is the reference design. A motivated alternative is
graded by the same criteria and must still produce unchanged Bronze data, the
six minimum Silver tables, a PostgreSQL Gold model, the two exact ML tables,
the required results files, and end-to-end tracing.

## Part I — Data Management (60)

### 1. Import, source information, and safe repeated runs — 11

- **9–11:** verifies unchanged Bronze files and safe archive members; uses
  stable source identifiers and input hashes; records run facts and counts;
  and proves that a second run adds no duplicates or unstable identifiers.
- **6–8:** working import with small tracing, run-record, or repeated-run gaps.
- **3–5:** inputs load, but source evidence is incomplete or duplicates are
  possible.
- **0–2:** source files are edited, untracked, or loaded manually.

### 2. Cleaning and quality evidence — 14

- **12–14:** tested rules cover syndrome shape and weights, binary
  length/order/padding, shot/prediction alignment, QASM structure, and required
  companion files; accepted and rejected counts reconcile; every invalid row
  remains visible with its value, rule, action, and reason.
- **8–11:** useful cleaning and issue handling with limited gaps.
- **4–7:** validation is mostly ad hoc, incomplete, or notebook-only.
- **0–3:** values are silently deleted/changed, important QEC meanings are
  confused, or cleaning is postponed until Gold.

### 3. Discovery and Gold modeling — 13

- **11–13:** clearly defines row meanings, identifiers, relationships, weighted
  and repeated structures, and an evidence-based rejected relationship; Gold
  uses useful shared vocabulary without inventing cross-source matches; design
  choices and their costs are explained.
- **7–10:** sound core discovery and entities with smaller ambiguities.
- **4–6:** plausible model with weak discovery evidence, or Gold mostly copies
  Silver tables without meaningful relational design.
- **0–3:** one-table imports, unclear row meaning, or invented relationships.

### 4. Parquet, PostgreSQL, and ML input tables — 15

- **13–15:** all six minimum Silver tables have correct Parquet columns, types,
  grain, and trace IDs; Gold has appropriate keys, constraints, indexes, and an
  all-or-nothing load; both ML tables match their contracts and are produced
  through student-written Gold queries/views and export logic; `example_id`
  resolves to the relevant Gold record or records.
- **9–12:** the full path works with small schema, constraint, export, or tracing
  gaps.
- **4–8:** Silver or Gold is incomplete, relational rules are weak, or ML tables
  are unstable.
- **0–3:** required compatibility outputs are absent, or ML is built by
  re-reading Bronze/Silver instead of Gold.

### 5. Part I operation, tests, and analysis — 7

- **6–7:** one-command run; focused quality, contract, tracing, and repeated-run
  tests; all required Part I results files; three reproducible SQL analyses
  including a join across three or more Gold tables; concise design and
  troubleshooting documentation; clean-run evidence.
- **4–5:** reproducible work with small testing, analysis, or documentation
  gaps.
- **2–3:** several manual steps or weak analytical traceability.
- **0–1:** the teaching team cannot reproduce the outputs.

## Part II — AI/ML consumer (40)

### 6. Weighted syndrome model — 12

- **10–12:** correct supplied partitions and physical weights; weighted prior
  and one weighted linear model; required metrics and timings; clear,
  data-based interpretation of imbalance and the result.
- **7–9:** correct task and useful comparison with smaller weighting or
  evaluation gaps.
- **3–6:** model runs but uses a weak baseline, row-only metrics, or an unclear
  split.
- **0–2:** the required models are absent, physical weights are ignored, or the
  prepared-task input is bypassed.

### 7. Google combined decoder — 12

- **10–12:** distance-three and distance-five work is separate; all four
  supplied decoders are compared on identical test shots; the five-feature
  linear combined decoder is aligned correctly; validation-only choices and
  decoder-error overlap are explained honestly.
- **7–9:** functional comparison and combined decoder with limited analysis or
  evidence gaps.
- **3–6:** incomplete decoder alignment, missing comparisons, or distances are
  merged without a sound reason.
- **0–2:** required inputs are not used or comparisons use different examples.

### 8. Raw-detector prototype — 6

- **5–6:** the required bounded distance-three MLP runs on the fixed subset;
  bit order and split are correct; training/prediction time is recorded; the
  report clearly explains information hidden by a flat input.
- **3–4:** prototype runs with smaller representation, timing, or explanation
  gaps.
- **1–2:** prototype is incomplete or its input meaning is unclear.
- **0:** no functioning raw-detector experiment.

### 9. Repeatability, evaluation, and tests — 10

- **9–10:** one command regenerates model files, predictions, metrics, and run
  facts; input hashes, settings, feature order, splits, seeds, and timings are
  recorded; tests cover contracts, split use, weights, bit order/event counts,
  repeatability, and metrics; prediction IDs trace back through Part I.
- **6–8:** reproducible model pipeline with some missing metadata or tests.
- **3–5:** manual experiments or incomplete saved results.
- **0–2:** reported results cannot be regenerated.

## Grade caps

- Maximum 40 if Part I is absent.
- Maximum 60 if Part II is absent.
- Maximum 55 if unchanged Bronze files or the six minimum Silver outputs are absent.
- Maximum 60 if reruns duplicate business records, or execution depends on
  manually ordered notebook cells.
- Maximum 65 if invalid values are silently changed, if the ML tables bypass
  Gold, or if training reparses source archives.
- Maximum 70 if physical `quantity` weights are ignored.
- Maximum 70 if measurements, detector events, logical flips, predictions, and
  decoder errors are treated as interchangeable.
- Non-reproducible outputs receive no credit in the affected section.
