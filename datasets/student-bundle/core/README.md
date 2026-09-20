# Quantum data lake core dataset bundle

This directory is the complete, network-free input for the core assignment.
Do not edit these source files. The downloadable package uses `raw/` as its
packaging directory; the platform seeding command presents the same unchanged
files in the `bronze/` area of the object store.

Contents:

- `raw/source=qasmbench/qasmbench-qec.zip`: three QEC OpenQASM benchmarks;
- `raw/source=qec_syndromes/syndromes_dataset.zip`: simulated, aggregated
  surface-code syndrome sequences;
- `raw/source=google_qec/google-surface-code-curated.zip`: real measurement,
  detector-event, logical-outcome, and decoder-prediction files;
- `metadata/bundle-manifest.json`: byte sizes, SHA-256 checksums, source details,
  and intended teaching roles;
- `metadata/ATTRIBUTION.md`: source and licensing information.

Students should begin with the assignment brief and data-source guide, not by
opening every file manually. Large and nested files are intentional.
