# Data-source guide

This guide explains the supplied data without prescribing a target schema. You
must verify these statements against the release.

## Simulated surface-code syndromes — mandatory

`syndromes_dataset.zip` contains seven CSV files. Their names encode surface-code
distance, physical fault rate, and nominal sample count:

```text
d-3_pfr-0.001000_nb-10M.csv
```

The actual columns are:

```text
labels,syndromes,quantity
```

`syndromes` is a string representation of a nested tuple. In this release, it
represents four rounds, each containing four binary syndrome values. `quantity`
is the number of simulated shots represented by the aggregate row. Do not
materialize 70 million individual rows merely because the weighted total is 70
million.

Questions to investigate:

- Does the documented schema match the actual header?
- Does every parsed sequence have the same round/check shape and binary domain?
- Do quantities reconcile to the filename sample count?
- Can the same syndrome occur under both logical-error labels?
- What is the correct key if syndrome alone is not unique?
- Which aggregates must use `quantity` as a weight?
- Why must a model split keep an entire physical-fault-rate group together?

Source: <https://doi.org/10.5281/zenodo.11166209>.

## Google surface-code experiments — mandatory

`google-surface-code-curated.zip` contains four distance-three experiments and
one distance-five experiment. All use the X logical basis, 25 rounds, and
50,000 hardware shots. Directory names encode the code, basis, distance,
rounds, and processor-center coordinates.

Each experiment directory includes:

- `properties.yml`: shot, qubit, measurement, detector, distance, and round
  metadata;
- `measurements.b8`: bit-packed hardware measurements from which detector
  events and logical labels are derived;
- `sweep.b8`: bit-packed per-shot initialization/configuration bits;
- `detection_events.b8`: bit-packed derived detector outcomes;
- `obs_flips_actual.01`: one logical-observable flip label per shot;
- four `obs_flips_predicted_by_*.01` decoder outputs;
- ideal/noisy Stim circuits, layouts, and detector error models.

Stim `b8` records are byte-aligned. Bits within each byte are little-endian, and
padding bits at the end of a record are not measurements or detectors. Use
`properties.yml` to calculate the exact expected byte length before decoding.
The supplied starter has low-level file readers, but you must decide what one
row represents and what to do when a check fails.

A raw stabilizer measurement and a detector event are different. A detector
event is derived from a change or inconsistency between measurements across
space/time according to the circuit annotations.

Questions to investigate:

- Do filenames and properties agree?
- Does every companion file exist?
- Does every binary file length match shots times byte-aligned bits per shot?
- Does every `01` file contain one binary line per shot?
- What distinguishes an actual logical flip from a decoder mistake?
- Beyond the packed per-shot bits the Google `ml` table needs, what should
  `gold` do with detector events: one row per fired detector, or per-position
  summaries? Which supports the required analyses, and at what size?
- Which supplied decoders make different mistakes, and can a simple combined
  model use that disagreement to predict the actual label?
- Why should distances three and five use separate raw-detector models?

Source: <https://doi.org/10.5281/zenodo.6804040>. Dataset context:
<https://doi.org/10.1038/s41586-022-05434-1>.

## QASMBench QEC circuits — mandatory

The curated archive contains only:

```text
small/error_correctiond3_n5/
small/qec_en_n5/
small/qec_sm_n5/
```

`qec_sm_n5.qasm` is the clearest parity-check example. It uses three data qubits
and two ancillas. Two CNOT pairs measure the adjacent data-qubit parities into a
two-bit syndrome register. It injects an X error and applies a conditional X
correction for syndrome values 1, 2, and 3.

Source and transpiled variants are both present. OpenQASM allows multiple
quantum registers and custom gate definitions. Register-local index zero in a
data register is not the same qubit as index zero in an ancilla register. Gate
definition bodies are definitions, not additional executed operations.

At minimum, extract:

- stable member locator and file hash;
- benchmark and source/transpiled variant;
- declared quantum/classical register sizes;
- operation, measurement, and two-qubit counts;
- parity-check ancilla, data participants, and syndrome bit where explicit;
- syndrome-controlled correction value, gate, and target.

Do not simulate the circuits. Source: <https://github.com/pnnl/QASMBench>.

The circuit entities explain parity-check and correction relationships in
Gold. They do not contain labeled shots and do not identify the circuits used
by the Google experiments. Do not turn them into fabricated training examples.

## How the three sources meet the QEC requirement

- The syndrome archive contains repeated parity/stabilizer outcomes and the
  logical-error target produced by simulation.
- The Google archive contains hardware parity measurements, derived changes in
  parity across rounds (`detection_events.b8`), actual logical outcomes, and
  decoder predictions.
- QASMBench makes the parity-measurement circuit explicit: ancillas collect
  adjacent data-qubit parity into a syndrome register that controls recovery.

Together they cover parity acquisition, syndrome/detector representation,
logical labels, decoder outputs, and circuit semantics. They are complementary
sources, not one joinable experiment.

## Sources and licensing

Exact source pages, licenses, checksums, versions, and redistribution notes are
in `metadata/bundle-manifest.json` and `metadata/ATTRIBUTION.md`. Keep the source
name, archive-member path, and repeatable file/record hash with every record in
`silver`, `gold`, and `ml` so a result can be traced back to `bronze`.
