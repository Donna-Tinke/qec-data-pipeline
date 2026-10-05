"""Our Silver pipeline for the qec_syndromes source.

Reads the seven simulated d=3 surface-code CSVs from the Bronze zip, checks
every row, and writes silver/qec_syndromes/syndrome_observation.parquet.

One Silver row = one original aggregate CSV row. `quantity` stays a weight;
rows are never expanded into individual shots. Checking the zip hash against
the bundle manifest is register_sources' job - here we only hash it for tracing.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import minio_client
from quantum_lake_student.lake import read_parquet, write_parquet
from quantum_lake_student.models import Severity, StageResult, stable_record_hash
from quantum_lake_student.results import replace_source_rows

SOURCE_NAME = "qec_syndromes"
BRONZE_OBJECT = f"bronze/source={SOURCE_NAME}/syndromes_dataset.zip"
SILVER_OBJECT = f"silver/{SOURCE_NAME}/syndrome_observation.parquet"
DEFAULT_RESULTS_DIR = Path("results/part1")

EXPECTED_CSV_COUNT = 7
EXPECTED_DISTANCE = 3
ROUND_COUNT = 4
CHECK_COUNT = 4

# The README inside the zip documents `label`, but the CSVs actually use `labels`.
ACTUAL_HEADER = ("labels", "syndromes", "quantity")
DOCUMENTED_HEADER = ("label", "syndromes", "quantity")

# d-3_pfr-0.001000_nb-10M.csv -> distance 3, fault rate 0.001, 10,000,000 samples
FILENAME_PATTERN = re.compile(
    r"^d-(?P<distance>\d+)_pfr-(?P<fault_rate>\d+(?:\.\d+)?)"
    r"_nb-(?P<samples>\d+)(?P<unit>[KM]?)\.csv$"
)
SAMPLE_UNITS = {"": 1, "K": 1_000, "M": 1_000_000}

# File-level rules
RULE_UNSAFE_MEMBER = f"{SOURCE_NAME}.unsafe_archive_member"
RULE_CSV_COUNT = f"{SOURCE_NAME}.csv_file_count"
RULE_FILENAME = f"{SOURCE_NAME}.filename_pattern"
RULE_DISTANCE = f"{SOURCE_NAME}.distance"
RULE_HEADER_DOCUMENTED = f"{SOURCE_NAME}.header_documented_label"
RULE_HEADER = f"{SOURCE_NAME}.header"
RULE_WEIGHTED_TOTAL = f"{SOURCE_NAME}.weighted_total"
# Row-level rules
RULE_COLUMN_COUNT = f"{SOURCE_NAME}.column_count"
RULE_LABEL_DOMAIN = f"{SOURCE_NAME}.label_domain"
RULE_SYNDROME_PARSE = f"{SOURCE_NAME}.syndrome_parse"
RULE_SYNDROME_SHAPE = f"{SOURCE_NAME}.syndrome_shape"
RULE_SYNDROME_DOMAIN = f"{SOURCE_NAME}.syndrome_domain"
RULE_QUANTITY = f"{SOURCE_NAME}.quantity"
RULE_DUPLICATE_KEY = f"{SOURCE_NAME}.duplicate_syndrome_label"

ACTION_REJECTED = "rejected"
ACTION_KEPT = "kept"

SYNDROME_OBSERVATION_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("experiment_id", pa.string()),
        ("physical_fault_rate", pa.float64()),
        ("syndrome_bits", pa.binary()),
        ("round_count", pa.int32()),
        ("check_count", pa.int32()),
        ("logical_error_label", pa.bool_()),
        ("quantity", pa.int64()),
    ]
)

TRACE_SCHEMA = pa.schema(
    [
        ("source_record_id", pa.string()),
        ("source_name", pa.string()),
        ("bronze_object", pa.string()),
        ("archive_member", pa.string()),
        ("record_locator", pa.string()),
        ("input_sha256", pa.string()),
    ]
)

ISSUE_SCHEMA = pa.schema(
    [
        ("issue_id", pa.string()),
        ("run_id", pa.string()),
        ("source_record_id", pa.string()),
        ("rule_id", pa.string()),
        ("severity", pa.string()),
        ("observed_value", pa.string()),
        ("action", pa.string()),
        ("reason", pa.string()),
    ]
)


class RowRejected(ValueError):
    """A CSV row failed a check and must stay out of Silver."""

    def __init__(self, rule_id: str, reason: str) -> None:
        super().__init__(reason)
        self.rule_id = rule_id
        self.reason = reason


@dataclass(frozen=True)
class FileMetadata:
    experiment_id: str
    distance: int
    physical_fault_rate: float
    nominal_sample_count: int


@dataclass(frozen=True)
class Issue:
    """One data_issues row, before the run_id is attached."""

    rule_id: str
    severity: Severity
    locator: str
    source_record_id: str | None
    observed_value: str | None
    action: str
    reason: str

    def to_row(self, run_id: str) -> dict[str, Any]:
        # issue_id leaves out run_id so a rerun on the same input gives the same id
        return {
            "issue_id": stable_record_hash({"rule_id": self.rule_id, "locator": self.locator}),
            "run_id": run_id,
            "source_record_id": self.source_record_id,
            "rule_id": self.rule_id,
            "severity": self.severity.value,
            "observed_value": self.observed_value,
            "action": self.action,
            "reason": self.reason,
        }


@dataclass
class CsvResult:
    observations: list[dict[str, Any]] = field(default_factory=list)
    trace_rows: list[dict[str, Any]] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)
    checks: dict[str, Any] = field(default_factory=dict)


@dataclass
class BuildResult:
    observations: list[dict[str, Any]]
    trace_rows: list[dict[str, Any]]
    issues: list[Issue]
    checks: dict[str, Any]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_record_id(archive_member: str, line_number: int) -> str:
    # Depends only on the member path and the CSV line, never on run time/order
    return f"{SOURCE_NAME}:{archive_member}:line={line_number}"


def check_member_name(name: str) -> None:
    # zip-slip guard: an unsafe member must stop the run (brief, "Data discovery and cleaning")
    pure = PurePosixPath(name)
    if name.startswith(("/", "\\")) or pure.is_absolute() or ".." in pure.parts:
        raise ValueError(f"{RULE_UNSAFE_MEMBER}: unsafe archive member path {name!r}")


def parse_filename(member: str) -> FileMetadata | None:
    stem_name = PurePosixPath(member).name
    match = FILENAME_PATTERN.match(stem_name)
    if match is None:
        return None
    return FileMetadata(
        experiment_id=PurePosixPath(member).stem,
        distance=int(match["distance"]),
        physical_fault_rate=float(match["fault_rate"]),
        nominal_sample_count=int(match["samples"]) * SAMPLE_UNITS[match["unit"]],
    )


def parse_label(raw: str) -> bool:
    value = raw.strip()
    if value not in {"0", "1"}:
        raise RowRejected(RULE_LABEL_DOMAIN, f"labels must be 0 or 1, got {raw!r}")
    return value == "1"


def parse_quantity(raw: str) -> int:
    value = raw.strip()
    if not value.isdigit():
        raise RowRejected(RULE_QUANTITY, f"quantity must be a positive integer, got {raw!r}")
    quantity = int(value)
    if quantity <= 0:
        raise RowRejected(RULE_QUANTITY, f"quantity must be greater than zero, got {quantity}")
    return quantity


def parse_syndrome(raw: str) -> tuple[int, ...]:
    """Flatten '((r0c0, ..), .., (r3c0, ..))' into 16 bits, round first then check."""
    try:
        # literal_eval only accepts Python literals, so no code in the CSV can run
        rounds = ast.literal_eval(raw.strip())
    except (ValueError, SyntaxError, MemoryError, RecursionError) as err:
        raise RowRejected(RULE_SYNDROME_PARSE, f"syndromes is not a tuple literal: {err}") from err

    if not isinstance(rounds, (tuple, list)) or len(rounds) != ROUND_COUNT:
        raise RowRejected(
            RULE_SYNDROME_SHAPE,
            f"syndromes must have {ROUND_COUNT} rounds, got "
            f"{len(rounds) if isinstance(rounds, (tuple, list)) else type(rounds).__name__}",
        )
    bits: list[int] = []
    for round_index, checks in enumerate(rounds):
        if not isinstance(checks, (tuple, list)) or len(checks) != CHECK_COUNT:
            raise RowRejected(
                RULE_SYNDROME_SHAPE,
                f"round {round_index} must have {CHECK_COUNT} checks, got {checks!r}",
            )
        for value in checks:
            # bool is a subclass of int, so rule out True/False explicitly
            if isinstance(value, bool) or not isinstance(value, int) or value not in (0, 1):
                raise RowRejected(
                    RULE_SYNDROME_DOMAIN,
                    f"syndrome values must be 0 or 1, got {value!r} in round {round_index}",
                )
            bits.append(value)
    return tuple(bits)


def _file_issue(
    rule_id: str,
    severity: Severity,
    member: str,
    reason: str,
    *,
    observed_value: str | None = None,
    action: str = ACTION_REJECTED,
) -> Issue:
    return Issue(
        rule_id=rule_id,
        severity=severity,
        locator=f"{SOURCE_NAME}:{member}",
        source_record_id=None,
        observed_value=observed_value,
        action=action,
        reason=reason,
    )


def process_csv(member: str, data: bytes, *, input_sha256: str) -> CsvResult:
    """Check one fault-rate CSV and turn its valid rows into Silver rows."""
    result = CsvResult()
    lines = data.decode("utf-8").splitlines()
    data_line_count = max(len(lines) - 1, 0)
    checks: dict[str, Any] = {"archive_member": member, "rows_read": data_line_count}
    result.checks = checks

    def reject_file(issue: Issue) -> CsvResult:
        result.issues.append(issue)
        checks.update(rows_accepted=0, rows_rejected=data_line_count, file_rejected=issue.rule_id)
        return result

    metadata = parse_filename(member)
    if metadata is None:
        return reject_file(
            _file_issue(
                RULE_FILENAME, Severity.ERROR, member,
                "filename does not match d-<distance>_pfr-<fault rate>_nb-<samples>.csv",
                observed_value=PurePosixPath(member).name,
            )
        )
    checks.update(
        experiment_id=metadata.experiment_id,
        physical_fault_rate=metadata.physical_fault_rate,
        nominal_sample_count=metadata.nominal_sample_count,
    )
    if metadata.distance != EXPECTED_DISTANCE:
        return reject_file(
            _file_issue(
                RULE_DISTANCE, Severity.ERROR, member,
                f"only distance {EXPECTED_DISTANCE} is expected (4x4 syndromes)",
                observed_value=str(metadata.distance),
            )
        )

    header = tuple(next(csv.reader([lines[0]]))) if lines else ()
    header = tuple(column.strip() for column in header)
    checks["header"] = list(header)
    if header == ACTUAL_HEADER:
        # expected: the data disagrees with its README; we keep using the real header
        result.issues.append(
            _file_issue(
                RULE_HEADER_DOCUMENTED, Severity.WARNING, member,
                "README documents a 'label' column but the CSV header uses 'labels'; "
                "the actual header is used",
                observed_value=lines[0],
                action=ACTION_KEPT,
            )
        )
    elif header != DOCUMENTED_HEADER:
        return reject_file(
            _file_issue(
                RULE_HEADER, Severity.ERROR, member,
                f"header must be {','.join(ACTUAL_HEADER)}",
                observed_value=lines[0] if lines else "",
            )
        )

    seen_keys: set[tuple[tuple[int, ...], bool]] = set()
    labels_by_syndrome: dict[tuple[int, ...], set[bool]] = defaultdict(set)
    label_weights: Counter[bool] = Counter()
    rejected = 0

    for line_number, line in enumerate(lines[1:], start=2):
        record_id = source_record_id(member, line_number)
        # rejected rows are traced too, so their data_issues rows can be followed back
        result.trace_rows.append(
            {
                "source_record_id": record_id,
                "source_name": SOURCE_NAME,
                "bronze_object": BRONZE_OBJECT,
                "archive_member": member,
                "record_locator": f"line={line_number}",
                "input_sha256": input_sha256,
            }
        )
        try:
            fields = next(csv.reader([line]), [])
            if len(fields) != len(ACTUAL_HEADER):
                raise RowRejected(
                    RULE_COLUMN_COUNT,
                    f"expected {len(ACTUAL_HEADER)} columns, got {len(fields)}",
                )
            raw_label, raw_syndrome, raw_quantity = fields
            label = parse_label(raw_label)
            bits = parse_syndrome(raw_syndrome)
            quantity = parse_quantity(raw_quantity)
        except RowRejected as err:
            rejected += 1
            result.issues.append(
                Issue(
                    rule_id=err.rule_id,
                    severity=Severity.ERROR,
                    locator=record_id,
                    source_record_id=record_id,
                    observed_value=line,
                    action=ACTION_REJECTED,
                    reason=err.reason,
                )
            )
            continue

        key = (bits, label)
        if key in seen_keys:
            # still one faithful CSV row, but Gold/ML keys on (experiment, syndrome, label)
            result.issues.append(
                Issue(
                    rule_id=RULE_DUPLICATE_KEY,
                    severity=Severity.WARNING,
                    locator=record_id,
                    source_record_id=record_id,
                    observed_value=line,
                    action=ACTION_KEPT,
                    reason="same syndrome and label already appeared earlier in this file",
                )
            )
        seen_keys.add(key)
        labels_by_syndrome[bits].add(label)
        label_weights[label] += quantity

        result.observations.append(
            {
                "source_record_id": record_id,
                "experiment_id": metadata.experiment_id,
                "physical_fault_rate": metadata.physical_fault_rate,
                "syndrome_bits": bytes(bits),
                "round_count": ROUND_COUNT,
                "check_count": CHECK_COUNT,
                "logical_error_label": label,
                "quantity": quantity,
            }
        )

    accepted_weight = label_weights[False] + label_weights[True]
    if accepted_weight != metadata.nominal_sample_count:
        result.issues.append(
            _file_issue(
                RULE_WEIGHTED_TOTAL, Severity.WARNING, member,
                f"accepted quantities sum to {accepted_weight}, "
                f"filename says {metadata.nominal_sample_count}",
                observed_value=str(accepted_weight),
                action=ACTION_KEPT,
            )
        )

    checks.update(
        rows_accepted=len(result.observations),
        rows_rejected=rejected,
        weighted_total=accepted_weight,
        weighted_total_matches_filename=accepted_weight == metadata.nominal_sample_count,
        weight_label_false=label_weights[False],
        weight_label_true=label_weights[True],
        distinct_syndromes=len(labels_by_syndrome),
        # valid, expected case: the same syndrome was seen with both logical outcomes
        syndromes_with_both_labels=sum(1 for labels in labels_by_syndrome.values() if len(labels) == 2),
    )
    return result


def build_silver_rows(archive_bytes: bytes, *, input_sha256: str) -> BuildResult:
    """Process every CSV in the syndrome zip. Raises if the archive is unsafe."""
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = sorted(info.filename for info in archive.infolist() if not info.is_dir())
        for name in members:
            check_member_name(name)
        csv_members = [name for name in members if name.lower().endswith(".csv")]
        if not csv_members:
            raise ValueError(f"{BRONZE_OBJECT} contains no CSV files")

        observations: list[dict[str, Any]] = []
        trace_rows: list[dict[str, Any]] = []
        issues: list[Issue] = []
        file_checks: list[dict[str, Any]] = []
        for member in csv_members:
            processed = process_csv(member, archive.read(member), input_sha256=input_sha256)
            observations.extend(processed.observations)
            trace_rows.extend(processed.trace_rows)
            issues.extend(processed.issues)
            file_checks.append(processed.checks)

    if len(csv_members) != EXPECTED_CSV_COUNT:
        issues.append(
            _file_issue(
                RULE_CSV_COUNT, Severity.WARNING, BRONZE_OBJECT,
                f"expected {EXPECTED_CSV_COUNT} fault-rate CSV files",
                observed_value=str(len(csv_members)),
                action=ACTION_KEPT,
            )
        )

    rows_read = sum(check["rows_read"] for check in file_checks)
    rows_accepted = sum(check["rows_accepted"] for check in file_checks)
    rows_rejected = sum(check["rows_rejected"] for check in file_checks)
    checks = {
        "source_name": SOURCE_NAME,
        "bronze_object": BRONZE_OBJECT,
        "input_sha256": input_sha256,
        "archive_members": members,
        "csv_file_count": len(csv_members),
        "rows_read": rows_read,
        "rows_accepted": rows_accepted,
        "rows_rejected": rows_rejected,
        "rows_reconcile": rows_read == rows_accepted + rows_rejected,
        "weighted_total": sum(check.get("weighted_total", 0) for check in file_checks),
        "issue_counts": dict(sorted(Counter(issue.rule_id for issue in issues).items())),
        "files": file_checks,
    }
    return BuildResult(observations, trace_rows, issues, checks)


def read_bronze_archive(settings: Settings) -> bytes:
    if settings.lake_backend == "local":
        for area in ("bronze", "raw"):
            candidate = settings.local_lake_root / area / f"source={SOURCE_NAME}" / "syndromes_dataset.zip"
            if candidate.exists():
                return candidate.read_bytes()
        raise FileNotFoundError(f"Could not find {BRONZE_OBJECT} under {settings.local_lake_root}")

    client = minio_client(settings)
    response = client.get_object(settings.s3_bucket, BRONZE_OBJECT)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def write_silver_table(table: pa.Table, settings: Settings) -> str:
    return write_parquet(table, SILVER_OBJECT, settings)


def read_silver_table(settings: Settings) -> pa.Table:
    return read_parquet(SILVER_OBJECT, settings)


def run(
    run_id: str,
    settings: Settings | None = None,
    results_dir: Path = DEFAULT_RESULTS_DIR,
) -> StageResult:
    """Build the syndrome Silver table plus our rows in the shared results tables."""
    if settings is None:
        settings = Settings.from_environment()
    result = StageResult(stage=f"silver.{SOURCE_NAME}", run_id=run_id)

    archive_bytes = read_bronze_archive(settings)
    built = build_silver_rows(archive_bytes, input_sha256=sha256_bytes(archive_bytes))

    write_silver_table(
        pa.Table.from_pylist(built.observations, schema=SYNDROME_OBSERVATION_SCHEMA), settings
    )
    replace_source_rows(
        results_dir / "source_trace.parquet",
        built.trace_rows,
        TRACE_SCHEMA,
        is_same_source=lambda row: row["source_name"] == SOURCE_NAME,
    )
    replace_source_rows(
        results_dir / "data_issues.parquet",
        [issue.to_row(run_id) for issue in built.issues],
        ISSUE_SCHEMA,
        is_same_source=lambda row: str(row["rule_id"]).startswith(f"{SOURCE_NAME}."),
    )
    # outcome of every check, incl. the ones that passed (brief: "Record the outcome of every check")
    checks_path = results_dir / "checks" / f"{SOURCE_NAME}.json"
    checks_path.parent.mkdir(parents=True, exist_ok=True)
    checks_path.write_text(json.dumps(built.checks, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result.input_count = built.checks["rows_read"]
    result.output_count = built.checks["rows_accepted"]
    result.issue_count = len(built.issues)
    result.finish()
    return result


if __name__ == "__main__":
    stage = run("manual")
    print(
        f"{stage.stage}: read {stage.input_count} rows, wrote {stage.output_count}, "
        f"{stage.issue_count} issues"
    )
