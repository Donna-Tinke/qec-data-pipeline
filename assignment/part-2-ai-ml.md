# Part II: AI/ML as a consumer of the data pipeline

> **Model performance is not part of the grade.** You are not graded on
> outperforming the supplied decoders or achieving a particular score. Marks
> are based on correct use of the prepared data, meaningful baselines,
> repeatability, evaluation, testing, and honest interpretation.

## Purpose and time

Part I turns the different QEC files into checked, documented tables. Part II
demonstrates that this pipeline can reliably serve an AI/ML consumer. It reads
the two Parquet tables from Part I's `ml` area and nothing else: not Gold, not
Silver, and never the supplied archives.

Plan about **16–21 team hours**, starting in the second half of week 3 and
finishing in week 4. Every task below is required, but each is deliberately
small. The focus remains on the data hand-off and repeatable evaluation.

## Required training inputs

- `ml_syndrome_decoder_example`: 75,598 aggregate examples representing 70
  million simulated observations. Each row has 16 ordered binary syndrome
  values, a logical-error label, `sample_weight=quantity`, and a supplied
  physical-fault-rate split.
- `ml_google_decoder_example`: 250,000 hardware shots. Each row has packed raw
  detector bits, detector-event count, four supplied decoder predictions, the
  actual logical-outcome label, and a supplied shot split.

The exact columns and split rules are in
[required-ml-tables.md](required-ml-tables.md). Use the supplied model-input
helpers. QASMBench has no labeled shots, so do not create artificial training
examples from its circuits.

## Task A: weighted syndrome decoder

Predict `logical_error_label` from the ordered 4-by-4 syndrome sequence.

Run exactly these comparisons:

1. a weighted majority/prior baseline; and
2. one weighted linear classifier, such as logistic regression.

Use `sample_weight` when fitting if the selected implementation supports it,
and use the physical weights in all reported metrics. Do not expand the 70
million represented observations into individual rows. Use training data for
fitting, validation data for any setting or threshold choice, and the supplied
test rows once for final evaluation.

## Task B: supplied and combined Google decoders

For distance three and distance five separately:

1. calculate a majority/prior baseline;
2. evaluate each of the four supplied decoder predictions on the same test
   shots; and
3. fit one simple linear combined decoder using these five inputs:
   normalized detector-event density and the four supplied predictions.

Do not merge the raw distance-three and distance-five detector vectors: they
have different widths. Choose any classifier threshold using validation data
only. Briefly describe whether the supplied decoders make the same or different
mistakes and how that affects the combined model.

## Task C: bounded raw-detector prototype

Train one small multilayer perceptron (MLP) directly on unpacked detector bits.
Use only distance-three rows satisfying:

```text
distance = 3 AND shot_index < 12_500
```

This gives a fixed, bounded subset across the four distance-three experiments.
Each row has 200 detector-bit inputs. Use the supplied split and record training
and prediction time.

This is a pipeline-consumer check, not an architecture competition. In your own
words, explain what a flat vector of 200 bits does not show explicitly—for
example detector position, neighborhood, or change over QEC rounds—and why that
limits the prototype. You are not required to design or research a more complex
model.

## Metrics

For every applicable comparison, report:

- logical-error rate (the main metric, physically weighted for Task A);
- balanced accuracy;
- Brier score when a probability is available;
- training time; and
- prediction time.

Use identical test examples when comparing models. A supplied decoder may not
provide a probability; record Brier score as not applicable instead of
inventing one.

## Required procedure

- Validate both input-table schemas before training.
- Use only features returned by the supplied helpers.
- Fit preprocessing and model parameters on training data.
- Use validation data for choices and thresholds.
- Use test data only for the final reported metrics.
- Use physical `sample_weight` for Task A fitting where supported and for all
  Task A evaluation metrics.
- Fix random seeds and dependency versions.
- One command must regenerate fitted models and all files under
  `results/part2/`.

## Required saved results

Submit:

1. importable training and evaluation code;
2. fixed ML dependency versions and random seeds;
3. fitted model files for the required learned models;
4. `results/part2/predictions.parquet`, containing at least `example_id`,
   `model_id`, `label`, `prediction`, nullable `probability`, and `split`;
5. `results/part2/metrics.json` with the required metrics by task, model, and
   relevant distance;
6. `results/part2/run.json` with the data-release version, hashes of both ML
   input tables, code revision, dependency versions, feature order, split rule,
   random seed, and timings;
7. `results/part2/report.md`, briefly explaining the inputs and targets, why
   each simple model fits its table, results, discarded information, and
   limitations; and
8. automated tests for input columns/types, split use, weights, packed-bit
   order and event counts, repeatability, and metric calculation.

No external reading or cited model justification is required. Base the short
report on the supplied data, your implementation, and the observed results.

## Definition of done

From a clean environment, one command reads only the two ML Parquet tables,
regenerates the fitted models and `results/part2/`, uses the supplied splits,
and reproduces the reported metrics. Every prediction has an `example_id` that
Part I can resolve to the relevant Gold record and then to the source data.

Again, **numerical model performance is not graded**. Correct data use,
repeatability, evaluation, testing, and honest conclusions are graded.
