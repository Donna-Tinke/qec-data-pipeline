#!/usr/bin/env python3
"""Check assignment documentation for broken links and stale scope claims."""

from __future__ import annotations

import re
from pathlib import Path


INFRASTRUCTURE = Path(__file__).resolve().parents[1]
PROJECT = INFRASTRUCTURE.parent
ASSIGNMENT = INFRASTRUCTURE / "assignment"

MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
MARKDOWN_FILES = sorted(list(ASSIGNMENT.glob("*.md")) + [INFRASTRUCTURE / "README.md"])

# Instructor documents are present only in the complete repository, not the
# self-contained student package.
INSTRUCTOR_FILES = [
    PROJECT / "README.md",
    PROJECT / "IMPLEMENTATION_PLAN.md",
    INFRASTRUCTURE / "datasets" / "README.md",
    PROJECT / "example-solution" / "README.md",
]
if all(path.is_file() for path in INSTRUCTOR_FILES):
    MARKDOWN_FILES = sorted(
        MARKDOWN_FILES
        + list((PROJECT / "docs").glob("*.md"))
        + list((PROJECT / "example-solution" / "docs").glob("*.md"))
        + list((PROJECT / "visualization-handoff").glob("*.md"))
        + INSTRUCTOR_FILES
    )


def verify_relative_links(path: Path, text: str) -> None:
    for target in MARKDOWN_LINK.findall(text):
        target = target.strip("<>").split("#", 1)[0]
        if not target or "://" in target or target.startswith("mailto:"):
            continue
        resolved = (path.parent / target).resolve()
        if not resolved.exists():
            raise AssertionError(f"Broken relative link in {path}: {target}")


def require(path: Path, *phrases: str) -> None:
    text = path.read_text(encoding="utf-8")
    missing = [phrase for phrase in phrases if phrase not in text]
    if missing:
        raise AssertionError(f"{path} misses required phrases: {missing}")


def reject(path: Path, *phrases: str) -> None:
    text = path.read_text(encoding="utf-8").lower()
    found = [phrase for phrase in phrases if phrase.lower() in text]
    if found:
        raise AssertionError(f"{path} contains obsolete phrases: {found}")


def main() -> None:
    required_student_files = {
        "ASSIGNMENT_SPEC.md",
        "README.md",
        "brief.md",
        "data-sources.md",
        "faq.md",
        "getting-started.md",
        "part-2-ai-ml.md",
        "plain-language-guide.md",
        "quantum-data-primer.md",
        "required-ml-tables.md",
        "rubric.md",
        "silver-tables.md",
        "submission-checklist.md",
    }
    present = {path.name for path in ASSIGNMENT.glob("*.md")}
    if present != required_student_files:
        raise AssertionError(
            "Unexpected student-document set: "
            f"missing={sorted(required_student_files - present)}, "
            f"extra={sorted(present - required_student_files)}"
        )

    for path in MARKDOWN_FILES:
        if not path.is_file():
            raise AssertionError(f"Missing documentation file: {path}")
        text = path.read_text(encoding="utf-8")
        verify_relative_links(path, text)
        if re.search(r"\b(?:TODO|TBD|yourusername|Your Name)\b", text):
            raise AssertionError(f"Placeholder text remains in {path}")

    require(
        ASSIGNMENT / "brief.md",
        "four weeks",
        "Part I — Data Management (about 60% of the work)",
        "Part II — AI/ML (about 40% of the work)",
        "You may use a different internal architecture",
        "results/part1/",
        "results/part2/",
        "Model performance is not part of the grade",
        "ml_syndrome_decoder_example",
        "ml_google_decoder_example",
    )
    require(
        ASSIGNMENT / "silver-tables.md",
        "syndrome_observation.parquet",
        "google_qec/experiment.parquet",
        "google_qec/shot.parquet",
        "qasmbench/circuit.parquet",
        "stabilizer_check.parquet",
        "conditional_correction.parquet",
        "source_trace.parquet",
    )
    require(
        ASSIGNMENT / "data-sources.md",
        "labels,syndromes,quantity",
        "measurements.b8",
        "detection_events.b8",
        "obs_flips_actual.01",
        "qec_sm_n5.qasm",
    )
    require(
        ASSIGNMENT / "quantum-data-primer.md",
        "Parity or stabilizer check",
        "Syndrome",
        "Repeated rounds and detector events",
        "Logical observable flip and decoder error",
        "Stim `b8` and `01` formats",
    )
    require(
        ASSIGNMENT / "required-ml-tables.md",
        "sample_weight",
        "detector_bits",
        "belief_matching_prediction",
        "google_data_split",
        "Each `example_id` must resolve",
    )
    require(
        ASSIGNMENT / "part-2-ai-ml.md",
        "Model performance is not part of the grade",
        "one weighted linear classifier",
        "one simple linear combined decoder",
        "shot_index < 12_500",
        "No external reading or cited model justification is required",
    )
    require(
        ASSIGNMENT / "rubric.md",
        "Part I contributes 60 points and Part II contributes 40",
        "Import, source information, and safe repeated runs — 11",
        "Cleaning and quality evidence — 14",
        "Discovery and Gold modeling — 13",
        "Parquet, PostgreSQL, and ML input tables — 15",
        "Part I operation, tests, and analysis — 7",
        "Weighted syndrome model — 12",
        "Google combined decoder — 12",
        "Raw-detector prototype — 6",
        "Repeatability, evaluation, and tests — 10",
        "Model performance is not graded",
    )

    for path in (
        ASSIGNMENT / "brief.md",
        ASSIGNMENT / "part-2-ai-ml.md",
        ASSIGNMENT / "rubric.md",
        ASSIGNMENT / "submission-checklist.md",
        ASSIGNMENT / "faq.md",
    ):
        reject(
            path,
            "literature-guide",
            "four primary sources",
            "literature-grounded",
            "nonlinear meta-decoder",
            "ROC AUC",
            "log loss",
            "one model card per",
        )

    student_text = "\n".join(
        path.read_text(encoding="utf-8") for path in ASSIGNMENT.glob("*.md")
    )
    stale_claims = [
        "Use all four core source objects",
        "Q-fid noise-adaptive data",
        "QASMBench small tier",
        "fidelity versus circuit depth",
        "QMapDataset",
        "Athens mapping",
        "QLDPC",
        "two to three weeks per part",
        "Part I and Part II each contribute 50",
    ]
    found = [claim for claim in stale_claims if claim.lower() in student_text.lower()]
    if found:
        raise AssertionError(f"Stale assignment claims remain: {found}")

    out_of_scope_terms = {
        "data leakage": r"\bdata leakage\b|\bleakage\b",
        "schema drift": r"\bschema drift\b",
        "old lifecycle paths": r"`(?:imported|cleaned|rejected)/",
    }
    for label, pattern in out_of_scope_terms.items():
        if re.search(pattern, student_text, re.I):
            raise AssertionError(f"Out-of-scope {label} wording remains")

    readme = (ASSIGNMENT / "README.md").read_text(encoding="utf-8")
    if readme.index("quantum-data-primer.md") > readme.index("data-sources.md"):
        raise AssertionError("The QEC primer must be read before the source guide")
    if readme.index("silver-tables.md") > readme.index("required-ml-tables.md"):
        raise AssertionError("The Silver contracts must precede the ML contracts")

    print(f"Documentation checks passed for {len(MARKDOWN_FILES)} Markdown files.")


if __name__ == "__main__":
    main()
