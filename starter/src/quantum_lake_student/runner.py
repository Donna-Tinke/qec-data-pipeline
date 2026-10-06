"""Pipeline runner orchestrating Part I stages and deliverables."""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.models import StageResult
from quantum_lake_student.stages import (
    build_ml_tables,
    load_postgres,
    prepare_data,
    register_sources,
)
from quantum_lake_student.stages.register_sources import bronze_run_facts


def get_code_revision() -> str:
    """Retrieve current Git commit revision hash."""
    # 1. Environment variable override
    for var in ("CODE_REVISION", "GIT_REVISION"):
        val = os.environ.get(var)
        if val and val.strip():
            return val.strip()

    # 2. File fallback (.git_revision in workspace or parent directory)
    for candidate in (
        Path(".git_revision"),
        Path(__file__).resolve().parents[2] / ".git_revision",
        Path(__file__).resolve().parents[3] / ".git_revision",
        Path("results/part1/.git_revision"),
    ):
        if candidate.exists():
            try:
                content = candidate.read_text(encoding="utf-8").strip()
                if content:
                    return content
            except Exception:
                pass

    # 3. Direct inspection of .git directory (pure Python, works without git CLI installed)
    for git_dir in (
        Path(".git"),
        Path(__file__).resolve().parents[2] / ".git",
        Path(__file__).resolve().parents[3] / ".git",
    ):
        if git_dir.exists():
            try:
                head_file = git_dir / "HEAD"
                if head_file.exists():
                    head_content = head_file.read_text(encoding="utf-8").strip()
                    if not head_content.startswith("ref:"):
                        return head_content
                    ref_subpath = head_content.split(":", 1)[1].strip()
                    ref_file = git_dir / ref_subpath
                    if ref_file.exists():
                        return ref_file.read_text(encoding="utf-8").strip()
                    packed = git_dir / "packed-refs"
                    if packed.exists():
                        for line in packed.read_text(encoding="utf-8").splitlines():
                            if line and not line.startswith(("#", "^")):
                                parts = line.strip().split()
                                if len(parts) == 2 and parts[1] == ref_subpath:
                                    return parts[0]
            except Exception:
                pass

    # 4. Git CLI fallback if available in PATH
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except Exception:
        pass

    return "unknown"


def generate_trace_examples(
    settings: Settings, results_dir: Path
) -> dict[str, Any]:
    """Generate sample end-to-end trace mapping for deliverables.
    
    ASSIGNMENT REQUIREMENT (brief.md lines 180-192):
    Every prediction in Part II must be traceable all the way back to the raw
    Bronze input bytes:
      ML Prediction (example_id) 
        -> Gold relational tables / views 
        -> Silver Parquet table (source_record_id) 
        -> Bronze raw archive (object, member file, line/byte locator, SHA256)
    
    The brief explicitly mandates:
      "Demonstrate the complete trace for one syndrome prediction and 
       one Google prediction in trace_examples.json."
    """
    from quantum_lake_student.gold import google_qec as gold_google_qec
    from quantum_lake_student.gold import qec_syndromes as gold_qec_syndromes

    with postgres_connection(settings) as conn:
        conn.execute("SET search_path TO gold, public")
        google_trace = gold_google_qec.trace_example(conn, results_dir)
        syndrome_trace = gold_qec_syndromes.trace_example(conn, results_dir)

    return {
        "google_qec": google_trace,
        "qec_syndromes": syndrome_trace,
    }


def collect_row_counts(
    prepare_result: StageResult | None = None,
    load_postgres_result: StageResult | None = None,
) -> dict[str, Any]:
    """Reconcile read, accepted, rejected, and loaded output table rows across Silver and Gold."""
    # 1. Silver Tables (Reconciled facts provided directly by source ingestion stages)
    silver_tables: dict[str, Any] = dict(prepare_result.table_counts) if prepare_result else {}

    # 2. Gold PostgreSQL Table Counts (Provided directly by relational load stage)
    gold_counts: dict[str, Any] = dict(load_postgres_result.table_counts) if load_postgres_result else {}

    return {
        "silver_tables": silver_tables,
        "gold_tables": gold_counts,
    }


def run_pipeline(settings: Settings) -> dict[str, Any]:
    """Execute the complete Part I pipeline across all stages."""
    run_id = f"run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    started_at = datetime.now(UTC)

    stages_results: dict[str, StageResult] = {}

    # Stage 1: Register Bronze Sources
    s1 = register_sources.run(run_id)
    stages_results["register_sources"] = s1

    # Stage 2: Prepare Silver Data
    s2 = prepare_data.run(run_id, settings)
    stages_results["prepare_data"] = s2

    # Stage 3: Load Gold PostgreSQL Model
    s3 = load_postgres.run(run_id, settings)
    stages_results["load_postgres"] = s3

    # Stage 4: Build ML Tables
    s4 = build_ml_tables.run(run_id, settings)
    stages_results["build_ml_tables"] = s4

    finished_at = datetime.now(UTC)
    duration = (finished_at - started_at).total_seconds()

    # Deliverables & Evidence
    results_dir = Path("results/part1")
    results_dir.mkdir(parents=True, exist_ok=True)

    bronze_facts = bronze_run_facts(settings)
    row_counts_data = collect_row_counts(
        prepare_result=stages_results.get("prepare_data"),
        load_postgres_result=stages_results.get("load_postgres"),
    )
    trace_examples_data = generate_trace_examples(settings, results_dir)

    run_json_data = {
        "run_id": run_id,
        "code_revision": get_code_revision(),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(duration, 3),
        "bronze_facts": bronze_facts,
        "stages": {
            name: {
                "stage": res.stage,
                "input_count": res.input_count,
                "output_count": res.output_count,
                "issue_count": res.issue_count,
                "started_at": res.started_at.isoformat() if res.started_at else None,
                "finished_at": res.finished_at.isoformat() if res.finished_at else None,
            }
            for name, res in stages_results.items()
        },
        "row_counts": row_counts_data,
    }

    (results_dir / "run.json").write_text(json.dumps(run_json_data, indent=2), encoding="utf-8")
    (results_dir / "row_counts.json").write_text(json.dumps(row_counts_data, indent=2), encoding="utf-8")
    (results_dir / "trace_examples.json").write_text(json.dumps(trace_examples_data, indent=2), encoding="utf-8")

    return {
        "run_id": run_id,
        "duration_seconds": duration,
        "stages": stages_results,
        "run_json": run_json_data,
    }
