"""
Pipeline stage for ingesting, validating, and preparing QASMBench QEC circuits.
"""

from __future__ import annotations

from collections.abc import Callable
import hashlib
import io
import json
from pathlib import Path
import re
from typing import Any
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import minio_client
from quantum_lake_student.models import QualityFinding, Severity, StageResult
from quantum_lake_student.results import replace_source_rows

# QASM validation rule identifier
RULE_PARSE_ERROR = "qasmbench.parse_error"


def is_same_source_trace(row: dict[str, Any]) -> bool:
    """Check if source trace row belongs to qasmbench."""
    return row.get("source_name") == "qasmbench" or str(row.get("source_record_id") or "").startswith("qasmbench:")


def is_same_data_issue(row: dict[str, Any]) -> bool:
    """Check if rule_id belongs to qasmbench."""
    return str(row.get("rule_id", "")).startswith("qasmbench")


CIRCUIT_SCHEMA = pa.schema([
    ("source_record_id", pa.string()),
    ("circuit_id", pa.string()),
    ("benchmark_name", pa.string()),
    ("variant", pa.string()),
    ("register_declarations", pa.string()),
    ("qubit_count", pa.int32()),
    ("measurement_count", pa.int32()),
    ("two_qubit_gate_count", pa.int32()),
])

STABILIZER_SCHEMA = pa.schema([
    ("source_record_id", pa.string()),
    ("circuit_id", pa.string()),
    ("check_id", pa.string()),
    ("ancilla_qubit", pa.string()),
    ("data_qubits", pa.list_(pa.string())),
    ("syndrome_bit", pa.string()),
])

CONDITIONAL_SCHEMA = pa.schema([
    ("source_record_id", pa.string()),
    ("circuit_id", pa.string()),
    ("condition_register", pa.string()),
    ("condition_value", pa.int64()),
    ("gate", pa.string()),
    ("target_qubit", pa.string()),
])

SOURCE_TRACE_SCHEMA = pa.schema([
    ("source_record_id", pa.string()),
    ("source_name", pa.string()),
    ("bronze_object", pa.string()),
    ("archive_member", pa.string()),
    ("record_locator", pa.string()),
    ("input_sha256", pa.string()),
])

DATA_ISSUES_SCHEMA = pa.schema([
    ("issue_id", pa.string()),
    ("run_id", pa.string()),
    ("source_record_id", pa.string()),
    ("rule_id", pa.string()),
    ("severity", pa.string()),
    ("observed_value", pa.string()),
    ("action", pa.string()),
    ("reason", pa.string()),
])


def get_file_buffer(settings: Settings, file_path: str, member_name: str | None = None) -> io.BytesIO:
    raw_bytes: bytes | None = None

    if settings.lake_backend == "local":
        full_path = (settings.local_lake_root / file_path).resolve()
        with open(full_path, "rb") as f:
            raw_bytes = f.read()
    else:
        client = minio_client(settings)
        response = client.get_object(settings.s3_bucket, file_path)
        try:
            raw_bytes = response.read()
        finally:
            response.close()
            response.release_conn()

    if raw_bytes is None:
        raise FileNotFoundError(f"Could not locate bronze file: {file_path}")

    if not member_name:
        return io.BytesIO(raw_bytes)

    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        return io.BytesIO(zf.read(member_name))





def build_source_record_id(source_name: str, archive_member: str, record_locator: str | None = None) -> str:
    if record_locator:
        return f"{source_name}:{archive_member}:{record_locator}"
    return f"{source_name}:{archive_member}"


def parse_qasm(
    content: str,
    source_name: str,
    bronze_object: str,
    archive_member: str,
    archive_sha256: str
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any] | None,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[QualityFinding],
]:
    """Parse qasm file and extract circuits, checks, and corrections."""
    findings: list[QualityFinding] = []
    source_records: list[dict[str, Any]] = []

    stem = Path(archive_member).stem
    circuit_id = stem
    variant = "transpiled" if "transpiled" in stem else "source"
    base_name = re.sub(r"_transpiled.*$", "", stem)
    benchmark_name = re.sub(r"_n\d+$", "", base_name)
    circ_source_rec_id = build_source_record_id(source_name, archive_member, None)
    source_records.append({
        "source_record_id": circ_source_rec_id,
        "source_name": source_name,
        "bronze_object": bronze_object,
        "archive_member": archive_member,
        "record_locator": None,
        "input_sha256": archive_sha256,
    })

    # Extract gate definitions (macro expansion)
    custom_gates: dict[str, dict[str, Any]] = {}
    gate_def_re = re.compile(
        r"gate\s+(\w+)(?:\s*\(([^)]*)\))?\s+([^\{]+)\{([^}]+)\}", re.DOTALL
    )
    for m in gate_def_re.finditer(content):
        gname, _, qargs_str, body = m.groups()
        qarg_names = [p.strip() for p in qargs_str.split(",") if p.strip()] if qargs_str else []
        clean_body = re.sub(r"//.*", "", body)
        stmts = [s.strip() for s in clean_body.split(";") if s.strip()]
        custom_gates[gname] = {"args": qarg_names, "body": stmts}

    # Extract sequential statements (including gate definition statements)
    raw_statements: list[str] = []
    gate_buffer: list[str] = []
    inside_gate_def = False
    for line in content.splitlines():
        clean_line = re.sub(r"//.*", "", line).strip()
        if not clean_line:
            continue
        if not inside_gate_def and ("gate " in clean_line or clean_line.startswith("gate")):
            inside_gate_def = True
            if "}" in clean_line:
                inside_gate_def = False
                before, _, after = clean_line.partition("}")
                raw_statements.append(before.strip() + " }")
                for piece in after.split(";"):
                    s = piece.strip()
                    if s:
                        raw_statements.append(s)
            else:
                gate_buffer = [clean_line]
            continue
        if inside_gate_def:
            if "}" in clean_line:
                inside_gate_def = False
                before, _, after = clean_line.partition("}")
                gate_buffer.append(before.strip() + " }")
                raw_statements.append(" ".join(gate_buffer))
                gate_buffer = []
                for piece in after.split(";"):
                    s = piece.strip()
                    if s:
                        raw_statements.append(s)
            else:
                gate_buffer.append(clean_line)
            continue
        for piece in clean_line.split(";"):
            stmt = piece.strip()
            if stmt:
                raw_statements.append(stmt)

    # Extract register declarations
    qregs: dict[str, int] = {}
    cregs: dict[str, int] = {}
    for stmt in raw_statements:
        if stmt.startswith("qreg"):
            m = re.match(r"^qreg\s+(\w+)\s*\[\s*(\d+)\s*\]", stmt)
            if m:
                qregs[m.group(1)] = int(m.group(2))
        elif stmt.startswith("creg"):
            m = re.match(r"^creg\s+(\w+)\s*\[\s*(\d+)\s*\]", stmt)
            if m:
                cregs[m.group(1)] = int(m.group(2))

    if not qregs:
        findings.append(
            QualityFinding(
                rule_id=RULE_PARSE_ERROR,
                severity=Severity.ERROR,
                source_system=source_name,
                source_record_locator=circ_source_rec_id,
                message="Circuit does not declare any quantum registers",
                observed_value=None,
            )
        )
        return source_records, None, [], [], findings

    def expand_instruction(op_name: str, args: list[str], stmt_loc: str) -> list[tuple[str, list[str], str]]:
        if op_name in custom_gates:
            gate_def = custom_gates[op_name]
            mapping = dict(zip(gate_def["args"], args))
            expanded: list[tuple[str, list[str], str]] = []
            for inner_stmt in gate_def["body"]:
                i_tokens = inner_stmt.split(None, 1)
                i_op = i_tokens[0]
                i_args = [mapping.get(a.strip(), a.strip()) for a in i_tokens[1].split(",")] if len(i_tokens) > 1 else []
                expanded.extend(expand_instruction(i_op, i_args, stmt_loc))
            return expanded
        return [(op_name, args, stmt_loc)]

    linear_timeline: list[dict[str, Any]] = []
    two_qubit_count = 0
    measurement_count = 0
    conditional_corrections: list[dict[str, Any]] = []

    for stmt_idx, stmt in enumerate(raw_statements, start=1):
        record_loc = f"stmt_{stmt_idx}"

        if stmt.startswith("measure"):
            m = re.match(r"measure\s+(\w+(?:\[\d+\])?)\s*->\s*(\w+(?:\[\d+\])?)", stmt)
            if m:
                q_op, c_op = m.group(1), m.group(2)
                if "[" not in q_op and q_op in qregs:
                    size = qregs[q_op]
                    measurement_count += size
                    for idx in range(size):
                        linear_timeline.append({
                            "type": "measure",
                            "qubit": f"{q_op}[{idx}]",
                            "cbit": f"{c_op}[{idx}]" if "[" not in c_op else c_op,
                            "stmt_loc": record_loc,
                        })
                else:
                    measurement_count += 1
                    linear_timeline.append({
                        "type": "measure",
                        "qubit": q_op,
                        "cbit": c_op,
                        "stmt_loc": record_loc,
                    })

        elif stmt.startswith("if"):
            m = re.match(r"if\s*\(\s*(\w+)\s*==\s*(\d+)\s*\)\s*(\w+)\s+([\w\[\]]+)", stmt)
            if m:
                reg, val_str, gate_name, target = m.groups()
                corr_source_rec_id = build_source_record_id(source_name, archive_member, record_loc)
                source_records.append({
                    "source_record_id": corr_source_rec_id,
                    "source_name": source_name,
                    "bronze_object": bronze_object,
                    "archive_member": archive_member,
                    "record_locator": record_loc,
                    "input_sha256": archive_sha256,
                })
                conditional_corrections.append({
                    "source_record_id": corr_source_rec_id,
                    "circuit_id": circuit_id,
                    "condition_register": reg,
                    "condition_value": int(val_str),
                    "gate": gate_name,
                    "target_qubit": target,
                })

        elif not (
            stmt.startswith("OPENQASM")
            or stmt.startswith("include")
            or stmt.startswith("qreg")
            or stmt.startswith("creg")
            or stmt.startswith("barrier")
            or stmt.startswith("reset")
            or stmt.startswith("gate")
            or stmt.startswith("opaque")
        ):
            tokens = stmt.split(None, 1)
            op = tokens[0]
            op_args = [a.strip() for a in tokens[1].split(",")] if len(tokens) > 1 else []
            expanded_ops = expand_instruction(op, op_args, record_loc)

            for e_op, e_args, e_loc in expanded_ops:
                if len(e_args) == 2 and "[" not in e_args[0] and e_args[0] in qregs and "[" not in e_args[1] and e_args[1] in qregs:
                    size = min(qregs[e_args[0]], qregs[e_args[1]])
                    two_qubit_count += size
                    for idx in range(size):
                        linear_timeline.append({
                            "type": "gate",
                            "gate": e_op,
                            "args": [f"{e_args[0]}[{idx}]", f"{e_args[1]}[{idx}]"],
                            "stmt_loc": e_loc,
                        })
                else:
                    if len(e_args) == 2:
                        two_qubit_count += 1
                    linear_timeline.append({
                        "type": "gate",
                        "gate": e_op,
                        "args": e_args,
                        "stmt_loc": e_loc,
                    })

    # Use register names to detect "explicit" parity checks (identifying ancilla/syndrome qubits)

    ancilla_regs = {r for r in qregs if r.lower().startswith("a")}
    syndrome_regs = {r for r in cregs if r.lower().startswith("syn")}
    data_regs = {r for r in qregs if r not in ancilla_regs}

    stabilizer_checks: list[dict[str, Any]] = []

    if ancilla_regs and syndrome_regs:
        ancilla_history: dict[str, dict[str, Any]] = {}

        for item in linear_timeline:
            if item["type"] == "gate":
                args = item["args"]
                stmt_loc = item["stmt_loc"]

                if len(args) == 2:
                    q0, q1 = args[0], args[1]
                    reg0 = q0.split("[")[0]
                    reg1 = q1.split("[")[0]

                    anc_target = None
                    data_source = None

                    if reg1 in ancilla_regs and reg0 in data_regs:
                        anc_target = q1
                        data_source = q0
                    elif reg0 in ancilla_regs and reg1 in data_regs:
                        anc_target = q0
                        data_source = q1

                    if anc_target:
                        if anc_target not in ancilla_history:
                            ancilla_history[anc_target] = {"data_qubits": [], "stmt_locs": set()}
                        if data_source not in ancilla_history[anc_target]["data_qubits"]:
                            ancilla_history[anc_target]["data_qubits"].append(data_source)
                        ancilla_history[anc_target]["stmt_locs"].add(stmt_loc)

            elif item["type"] == "measure":
                q = item["qubit"]
                c = item["cbit"]
                m_loc = item["stmt_loc"]
                q_reg = q.split("[")[0]
                c_reg = c.split("[")[0]

                if q_reg in ancilla_regs and c_reg in syndrome_regs:
                    if q in ancilla_history and ancilla_history[q]["data_qubits"]:
                        d_qubits = ancilla_history[q]["data_qubits"]
                        data_str = "-".join(sorted(d_qubits))
                        check_id = f"{q}_{data_str}_{c}"

                        raw_locs = list(ancilla_history[q]["stmt_locs"] | {m_loc})
                        sorted_locs = sorted(
                            raw_locs,
                            key=lambda s: int(s.split("_")[1]) if "_" in s and s.split("_")[1].isdigit() else s,
                        )
                        loc_repr = "+".join(sorted_locs)
                        stab_source_rec_id = build_source_record_id(
                            source_name, archive_member, loc_repr
                        )

                        source_records.append({
                            "source_record_id": stab_source_rec_id,
                            "source_name": source_name,
                            "bronze_object": bronze_object,
                            "archive_member": archive_member,
                            "record_locator": loc_repr,
                            "input_sha256": archive_sha256,
                        })

                        stabilizer_checks.append({
                            "source_record_id": stab_source_rec_id,
                            "circuit_id": circuit_id,
                            "check_id": check_id,
                            "ancilla_qubit": q,
                            "data_qubits": d_qubits,
                            "syndrome_bit": c,
                        })

                        ancilla_history[q] = {"data_qubits": [], "stmt_locs": set()}

    qubit_count = int(sum(qregs.values()))

    circuit_entry = {
        "source_record_id": circ_source_rec_id,
        "circuit_id": circuit_id,
        "benchmark_name": benchmark_name,
        "variant": variant,
        "register_declarations": json.dumps(
            {"cregs": dict(sorted(cregs.items())), "qregs": dict(sorted(qregs.items()))},
            sort_keys=True,
        ),
        "qubit_count": qubit_count,
        "measurement_count": int(measurement_count),
        "two_qubit_gate_count": int(two_qubit_count),
    }

    return source_records, circuit_entry, stabilizer_checks, conditional_corrections, findings


def write_lake_table(
    table: pa.Table,
    relative_path: str,
    settings: Settings,
) -> str:
    """Publish a Parquet table to the lake."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue()

    if settings.lake_backend == "local":
        target = settings.local_lake_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "wb") as f:
            f.write(data)
        return str(target)
    else:
        client = minio_client(settings)
        client.put_object(
            settings.s3_bucket,
            relative_path,
            io.BytesIO(data),
            len(data),
            content_type="application/octet-stream",
        )
        return f"s3://{settings.s3_bucket}/{relative_path}"


def read_lake_table(
    relative_path: str,
    settings: Settings,
) -> pa.Table:
    """Read a Parquet table from the lake."""
    if settings.lake_backend == "local":
        target = settings.local_lake_root / relative_path
        return pq.read_table(target)
    else:
        client = minio_client(settings)
        response = client.get_object(settings.s3_bucket, relative_path)
        try:
            return pq.read_table(io.BytesIO(response.read()))
        finally:
            response.close()
            response.release_conn()


def write_result_table(
    new_rows: list[dict[str, Any]],
    schema: pa.Schema,
    relative_path: str,
    settings: Settings,
    *,
    is_same_source: Callable[[dict[str, Any]], bool],
) -> Path:
    """Publish a results Parquet table to results/ preserving other sources."""
    target = Path(relative_path)
    replace_source_rows(
        path=target,
        new_rows=new_rows,
        schema=schema,
        is_same_source=is_same_source,
    )
    return target


def run_qasmbench_pipeline(
    settings: Settings,
    run_id: str = "default_run",
    results_dir: Path | str = "results/part1",
) -> dict[str, Any]:
    """Execute the full QASMBench ingestion, validation, and publishing pipeline."""
    archive_path = "bronze/source=qasmbench/qasmbench-qec.zip"
    archive_buf = get_file_buffer(settings, archive_path)
    archive_bytes = archive_buf.getvalue()
    archive_sha256 = hashlib.sha256(archive_bytes).hexdigest()

    all_findings: list[QualityFinding] = []

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        qasm_members = sorted([
            info.filename
            for info in zf.infolist()
            if not info.is_dir() and info.filename.endswith(".qasm") and info.filename.startswith("small/")
        ])

    all_source_records: list[dict[str, Any]] = []
    circuits: list[dict[str, Any]] = []
    stabilizer_checks: list[dict[str, Any]] = []
    conditional_corrections: list[dict[str, Any]] = []

    # Parse each QASM circuit
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        for member_name in qasm_members:
            try:
                raw_content = zf.read(member_name).decode("utf-8")
                s_recs, circ, stabs, conds, findings = parse_qasm(
                    content=raw_content,
                    source_name="qasmbench",
                    bronze_object=archive_path,
                    archive_member=member_name,
                    archive_sha256=archive_sha256
                )
                all_source_records.extend(s_recs)
                all_findings.extend(findings)
                if circ is not None:
                    circuits.append(circ)
                stabilizer_checks.extend(stabs)
                conditional_corrections.extend(conds)
            except Exception as err:
                all_findings.append(
                    QualityFinding(
                        rule_id=RULE_PARSE_ERROR,
                        severity=Severity.ERROR,
                        source_system="qasmbench",
                        source_record_locator=build_source_record_id(
                            "qasmbench", member_name, None
                        ),
                        message=f"Failed to parse QASM circuit: {err}",
                        observed_value=str(err),
                    )
                )

    # Deduplicate source records by source_record_id
    deduped_trace_map: dict[str, dict[str, Any]] = {}
    for rec in all_source_records:
        deduped_trace_map[rec["source_record_id"]] = rec
    unique_source_records = list(deduped_trace_map.values())

    # Format quality findings into data_issues schema
    issue_records: list[dict[str, Any]] = []
    for f in all_findings:
        issue_records.append({
            "issue_id": f"{run_id}:{f.rule_id}:{f.source_record_locator}",
            "run_id": run_id,
            "source_record_id": None if f.severity == Severity.ERROR else f.source_record_locator,
            "rule_id": f.rule_id,
            "severity": str(f.severity),
            "observed_value": str(f.observed_value) if f.observed_value is not None else None,
            "action": "exclude" if f.severity == Severity.ERROR else "warn",
            "reason": f.message,
        })

    # Write Silver tables to the lake
    circuit_table = pa.Table.from_pylist(circuits, schema=CIRCUIT_SCHEMA)
    circuit_loc = write_lake_table(
        circuit_table,
        "silver/qasmbench/circuit.parquet",
        settings,
    )

    stabilizer_table = pa.Table.from_pylist(stabilizer_checks, schema=STABILIZER_SCHEMA)
    stabilizer_loc = write_lake_table(
        stabilizer_table,
        "silver/qasmbench/stabilizer_check.parquet",
        settings,
    )

    conditional_table = pa.Table.from_pylist(conditional_corrections, schema=CONDITIONAL_SCHEMA)
    conditional_loc = write_lake_table(
        conditional_table,
        "silver/qasmbench/conditional_correction.parquet",
        settings,
    )

    results_path = Path(results_dir)

    # Write Results tables to results/part1/ (or injected results_dir)
    source_trace_loc = write_result_table(
        new_rows=unique_source_records,
        schema=SOURCE_TRACE_SCHEMA,
        relative_path=str(results_path / "source_trace.parquet"),
        settings=settings,
        is_same_source=is_same_source_trace,
    )

    data_issues_loc = write_result_table(
        new_rows=issue_records,
        schema=DATA_ISSUES_SCHEMA,
        relative_path=str(results_path / "data_issues.parquet"),
        settings=settings,
        is_same_source=is_same_data_issue,
    )

    output_count = len(circuits) + len(stabilizer_checks) + len(conditional_corrections)

    return {
        "input_count": len(qasm_members),
        "output_count": output_count,
        "circuit_count": len(circuits),
        "stabilizer_check_count": len(stabilizer_checks),
        "conditional_correction_count": len(conditional_corrections),
        "issue_count": len(issue_records),
        "source_trace_count": len(unique_source_records),
        "files": {
            "circuit": circuit_loc,
            "stabilizer_check": stabilizer_loc,
            "conditional_correction": conditional_loc,
            "source_trace": str(source_trace_loc),
            "data_issues": str(data_issues_loc),
        },
    }


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path | str = "results/part1",
) -> StageResult:
    """Execute the QASMBench data preparation stage."""
    if settings is None:
        settings = Settings.from_environment()

    result = StageResult(stage="silver.qasmbench", run_id=run_id)
    summary = run_qasmbench_pipeline(settings, run_id=run_id, results_dir=results_dir)

    result.input_count = summary["input_count"]
    result.output_count = summary["output_count"]
    result.issue_count = summary["issue_count"]
    result.finish()
    return result