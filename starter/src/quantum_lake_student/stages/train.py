"""Part II QEC model training.

Reads only the two Part I ML Parquet tables and regenerates results/part2/.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import pickle
import shutil
import subprocess
from pathlib import Path
from time import perf_counter

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, brier_score_loss
from sklearn.neural_network import MLPClassifier

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import minio_client
from quantum_lake_student.lake import read_parquet
from quantum_lake_student.ml import (
    GOOGLE_META_PREDICTION_COLUMNS,
    MODEL_SPLITS,
    google_data_split,
    google_meta_model_input,
    syndrome_data_split,
    syndrome_model_input,
    unpack_little_endian_bits,
    weighted_logical_error_rate,
)
from quantum_lake_student.models import StageResult

SEED = 42
SYNDROME_OBJECT = "ml/ml_syndrome_decoder_example.parquet"
GOOGLE_OBJECT = "ml/ml_google_decoder_example.parquet"
RESULTS_DIR = Path("results/part2")

SYNDROME_TYPES = [
    ("example_id", pa.string()), ("experiment_id", pa.string()),
    ("physical_fault_rate", pa.float64()), ("syndrome_bits", pa.binary()),
    ("round_count", pa.int32()), ("check_count", pa.int32()),
    ("logical_error_label", pa.bool_()), ("sample_weight", pa.int64()),
    ("data_split", pa.string()),
]
GOOGLE_TYPES = [
    ("example_id", pa.string()), ("experiment_id", pa.string()),
    ("shot_index", pa.int64()), ("distance", pa.int32()),
    ("rounds", pa.int32()), ("center_row", pa.int32()),
    ("center_col", pa.int32()), ("detector_count", pa.int32()),
    ("detector_event_count", pa.int32()), ("detector_bits", pa.binary()),
    ("belief_matching_prediction", pa.bool_()),
    ("correlated_matching_prediction", pa.bool_()),
    ("pymatching_prediction", pa.bool_()),
    ("tensor_network_contraction_prediction", pa.bool_()),
    ("actual_observable_flip", pa.bool_()), ("data_split", pa.string()),
]
PRED_SCHEMA = pa.schema([
    ("example_id", pa.string()), ("model_id", pa.string()),
    ("label", pa.bool_()), ("prediction", pa.bool_()),
    ("probability", pa.float64()), ("split", pa.string()),
])


def _split(rows):
    out = {name: [] for name in MODEL_SPLITS}
    for row in rows:
        out[row["data_split"]].append(row)
    return out


def _check_schema(table: pa.Table, expected, name: str, rows: int) -> None:
    if table.schema.names != [n for n, _ in expected]:
        raise ValueError(f"{name}: wrong columns")
    for field, (_, expected_type) in zip(table.schema, expected, strict=True):
        if field.type != expected_type:
            raise ValueError(f"{name}: {field.name} has type {field.type}, expected {expected_type}")
    if table.num_rows != rows:
        raise ValueError(f"{name}: expected {rows} rows, got {table.num_rows}")
    if any(table.column(i).null_count for i in range(table.num_columns)):
        raise ValueError(f"{name}: null values are not allowed")


def validate_inputs(syndrome: pa.Table, google: pa.Table):
    _check_schema(syndrome, SYNDROME_TYPES, "syndrome", 75_598)
    _check_schema(google, GOOGLE_TYPES, "google", 250_000)
    s_rows, g_rows = syndrome.to_pylist(), google.to_pylist()

    if len({r["example_id"] for r in s_rows}) != len(s_rows):
        raise ValueError("duplicate syndrome example_id")
    if len({r["example_id"] for r in g_rows}) != len(g_rows):
        raise ValueError("duplicate Google example_id")

    for r in s_rows:
        syndrome_model_input(r["syndrome_bits"])
        if r["data_split"] != syndrome_data_split(r["physical_fault_rate"]):
            raise ValueError("bad syndrome split")
        if r["round_count"] != 4 or r["check_count"] != 4 or r["sample_weight"] <= 0:
            raise ValueError("bad syndrome shape/weight")

    for r in g_rows:
        if r["data_split"] != google_data_split(r["shot_index"]):
            raise ValueError("bad Google split")
        expected = 200 if r["distance"] == 3 else 600 if r["distance"] == 5 else -1
        if r["rounds"] != 25 or r["detector_count"] != expected:
            raise ValueError("bad Google experiment shape")
        bits = unpack_little_endian_bits(r["detector_bits"], r["detector_count"])
        if sum(bits) != r["detector_event_count"]:
            raise ValueError("detector_event_count mismatch")
        google_meta_model_input(r)

    if {r["data_split"] for r in s_rows} != set(MODEL_SPLITS):
        raise ValueError("missing syndrome split")
    if {r["data_split"] for r in g_rows} != set(MODEL_SPLITS):
        raise ValueError("missing Google split")
    return s_rows, g_rows


def choose_threshold(y, probability, weights=None):
    best = (0.5, -1.0)
    for threshold in np.linspace(0.10, 0.90, 17):
        score = balanced_accuracy_score(
            y, probability >= threshold, sample_weight=weights
        )
        if score > best[1]:
            best = (float(threshold), float(score))
    return best[0]


def evaluate(y, pred, probability=None, weights=None, train_time=0.0, predict_time=0.0, threshold=None):
    ler = (
        float(np.mean(y != pred))
        if weights is None
        else float(weighted_logical_error_rate(y, pred, weights))
    )
    result = {
        "logical_error_rate": ler,
        "balanced_accuracy": float(balanced_accuracy_score(y, pred, sample_weight=weights)),
        "brier_score": None if probability is None else float(
            brier_score_loss(y, probability, sample_weight=weights)
        ),
        "training_seconds": float(train_time),
        "prediction_seconds": float(predict_time),
    }
    if threshold is not None:
        result["threshold"] = float(threshold)
    return result


def add_predictions(out, rows, model_id, y, pred, probability=None):
    for i, row in enumerate(rows):
        out.append({
            "example_id": row["example_id"],
            "model_id": model_id,
            "label": bool(y[i]),
            "prediction": bool(pred[i]),
            "probability": None if probability is None else float(probability[i]),
            "split": "test",
        })


def save_model(name: str, model) -> None:
    with (RESULTS_DIR / "models" / name).open("wb") as f:
        pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)


def run_task_a(rows, predictions):
    parts = _split(rows)

    def arrays(name):
        r = parts[name]
        x = np.asarray([syndrome_model_input(v["syndrome_bits"]) for v in r], dtype=float)
        y = np.asarray([v["logical_error_label"] for v in r], dtype=np.int8)
        w = np.asarray([v["sample_weight"] for v in r], dtype=float)
        return x, y, w

    x_train, y_train, w_train = arrays("train")
    x_val, y_val, w_val = arrays("validation")
    x_test, y_test, w_test = arrays("test")
    test_rows = parts["test"]

    start = perf_counter()
    prior = float(np.average(y_train, weights=w_train))
    train_time = perf_counter() - start
    start = perf_counter()
    prior_prob = np.full(len(y_test), prior)
    prior_pred = prior_prob >= 0.5
    predict_time = perf_counter() - start
    prior_metrics = evaluate(
        y_test, prior_pred, prior_prob, w_test, train_time, predict_time, 0.5
    )
    prior_metrics["train_positive_prior"] = prior
    add_predictions(predictions, test_rows, "task_a_weighted_prior", y_test, prior_pred, prior_prob)

    model = LogisticRegression(max_iter=500, random_state=SEED)
    start = perf_counter()
    model.fit(x_train, y_train, sample_weight=w_train)
    train_time = perf_counter() - start
    threshold = choose_threshold(y_val, model.predict_proba(x_val)[:, 1], w_val)
    start = perf_counter()
    prob = model.predict_proba(x_test)[:, 1]
    pred = prob >= threshold
    predict_time = perf_counter() - start
    model_metrics = evaluate(
        y_test, pred, prob, w_test, train_time, predict_time, threshold
    )
    add_predictions(predictions, test_rows, "task_a_weighted_logistic", y_test, pred, prob)
    save_model("task_a_weighted_logistic.pkl", model)

    return {
        "weighted_prior": prior_metrics,
        "weighted_logistic_regression": model_metrics,
        "row_counts": {k: len(v) for k, v in parts.items()},
        "weighted_observation_counts": {
            k: int(sum(r["sample_weight"] for r in v)) for k, v in parts.items()
        },
    }


def run_task_b(rows, distance, predictions):
    parts = _split([r for r in rows if r["distance"] == distance])
    train, val, test = parts["train"], parts["validation"], parts["test"]

    def xy(data):
        return (
            np.asarray([google_meta_model_input(r) for r in data], dtype=float),
            np.asarray([r["actual_observable_flip"] for r in data], dtype=np.int8),
        )

    x_train, y_train = xy(train)
    x_val, y_val = xy(val)
    x_test, y_test = xy(test)
    result = {"row_counts": {k: len(v) for k, v in parts.items()}}

    start = perf_counter()
    prior = float(np.mean(y_train))
    train_time = perf_counter() - start
    start = perf_counter()
    prior_prob = np.full(len(y_test), prior)
    prior_pred = prior_prob >= 0.5
    predict_time = perf_counter() - start
    result["majority_prior"] = evaluate(
        y_test, prior_pred, prior_prob, None, train_time, predict_time, 0.5
    )
    result["majority_prior"]["train_positive_prior"] = prior
    add_predictions(
        predictions, test, f"task_b_d{distance}_majority_prior", y_test, prior_pred, prior_prob
    )

    decoder_predictions = []
    for column in GOOGLE_META_PREDICTION_COLUMNS:
        start = perf_counter()
        pred = np.asarray([r[column] for r in test], dtype=bool)
        predict_time = perf_counter() - start
        decoder_predictions.append(pred)
        name = column.removesuffix("_prediction")
        result[name] = evaluate(y_test, pred, None, None, 0.0, predict_time)
        add_predictions(predictions, test, f"task_b_d{distance}_{name}", y_test, pred)

    matrix = np.column_stack(decoder_predictions)
    wrong = matrix != y_test[:, None]
    result["mistake_overlap"] = {
        "all_four_wrong_rate": float(np.mean(np.all(wrong, axis=1))),
        "at_least_one_wrong_rate": float(np.mean(np.any(wrong, axis=1))),
        "decoder_prediction_disagreement_rate": float(
            np.mean(np.any(matrix != matrix[:, [0]], axis=1))
        ),
    }

    model = LogisticRegression(max_iter=500, random_state=SEED)
    start = perf_counter()
    model.fit(x_train, y_train)
    train_time = perf_counter() - start
    threshold = choose_threshold(y_val, model.predict_proba(x_val)[:, 1])
    start = perf_counter()
    prob = model.predict_proba(x_test)[:, 1]
    pred = prob >= threshold
    predict_time = perf_counter() - start
    result["combined_logistic_regression"] = evaluate(
        y_test, pred, prob, None, train_time, predict_time, threshold
    )
    add_predictions(
        predictions, test, f"task_b_d{distance}_combined_logistic", y_test, pred, prob
    )
    save_model(f"task_b_d{distance}_combined_logistic.pkl", model)
    return result


def run_task_c(rows, predictions):
    parts = _split([
        r for r in rows if r["distance"] == 3 and r["shot_index"] < 12_500
    ])

    def xy(data):
        x = np.asarray([
            unpack_little_endian_bits(r["detector_bits"], r["detector_count"])
            for r in data
        ], dtype=np.float32)
        if x.shape[1] != 200:
            raise ValueError("Task C requires 200 detector inputs")
        y = np.asarray([r["actual_observable_flip"] for r in data], dtype=np.int8)
        return x, y

    x_train, y_train = xy(parts["train"])
    x_val, y_val = xy(parts["validation"])
    x_test, y_test = xy(parts["test"])

    model = MLPClassifier(
        hidden_layer_sizes=(32,), max_iter=20, batch_size=256, random_state=SEED
    )
    start = perf_counter()
    model.fit(x_train, y_train)
    train_time = perf_counter() - start
    threshold = choose_threshold(y_val, model.predict_proba(x_val)[:, 1])
    start = perf_counter()
    prob = model.predict_proba(x_test)[:, 1]
    pred = prob >= threshold
    predict_time = perf_counter() - start
    metrics = evaluate(
        y_test, pred, prob, None, train_time, predict_time, threshold
    )
    add_predictions(
        predictions, parts["test"], "task_c_d3_raw_detector_mlp", y_test, pred, prob
    )
    save_model("task_c_d3_raw_detector_mlp.pkl", model)

    return {
        "raw_detector_mlp": metrics,
        "row_counts": {k: len(v) for k, v in parts.items()},
        "feature_count": 200,
        "subset_rule": "distance = 3 AND shot_index < 12500",
    }


def lake_bytes(key: str, settings: Settings) -> bytes:
    if settings.lake_backend == "local":
        return (settings.local_lake_root / key).read_bytes()
    client = minio_client(settings)
    response = client.get_object(settings.s3_bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def code_revision() -> str:
    if os.getenv("CODE_REVISION"):
        return os.environ["CODE_REVISION"]
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable-in-container"


def write_json(name: str, value) -> None:
    (RESULTS_DIR / name).write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_report(metrics) -> None:
    a = metrics["task_a"]["weighted_logistic_regression"]
    c = metrics["task_c"]["raw_detector_mlp"]
    lines = [
        "# Part II QEC decoder report",
        "",
        "Task A uses the 16 ordered syndrome bits and physical sample weights.",
        f"Weighted logistic test logical-error rate: {a['logical_error_rate']:.6f}.",
        f"Weighted logistic balanced accuracy: {a['balanced_accuracy']:.6f}.",
        "",
        "Task B is evaluated separately for distance 3 and distance 5. The combined",
        "model uses only normalized detector-event density and the four supplied",
        "decoder predictions. Supplied decoder Brier scores are N/A because they",
        "provide binary predictions rather than probabilities.",
        "",
    ]
    for d in (3, 5):
        b = metrics["task_b"][f"distance_{d}"]
        o = b["mistake_overlap"]
        lines += [
            f"## Distance {d}",
            f"Combined logical-error rate: {b['combined_logistic_regression']['logical_error_rate']:.6f}.",
            f"All four decoders wrong: {o['all_four_wrong_rate']:.6f}.",
            f"At least one decoder wrong: {o['at_least_one_wrong_rate']:.6f}.",
            f"Decoder prediction disagreement: {o['decoder_prediction_disagreement_rate']:.6f}.",
            "The decoders do not make perfectly identical predictions, so the combined",
            "linear model can use some complementary information.",
            "",
        ]
    lines += [
        "## Raw-detector prototype",
        f"MLP logical-error rate: {c['logical_error_rate']:.6f}.",
        f"Training time: {c['training_seconds']:.3f} s.",
        f"Prediction time: {c['prediction_seconds']:.3f} s.",
        "The flat 200-bit vector does not explicitly encode detector position,",
        "neighborhood structure, or changes over QEC rounds. This limits the MLP.",
        "",
        "Models are fitted only on training data, thresholds are chosen only from",
        "validation data, and final metrics use only the supplied test split.",
    ]
    (RESULTS_DIR / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(model_run_id: str) -> StageResult:
    settings = Settings.from_environment()
    np.random.seed(SEED)

    if RESULTS_DIR.exists():
        shutil.rmtree(RESULTS_DIR)
    (RESULTS_DIR / "models").mkdir(parents=True)

    start = perf_counter()
    syndrome_table = read_parquet(SYNDROME_OBJECT, settings)
    google_table = read_parquet(GOOGLE_OBJECT, settings)
    syndrome_rows, google_rows = validate_inputs(syndrome_table, google_table)

    predictions = []
    metrics = {
        "task_a": run_task_a(syndrome_rows, predictions),
        "task_b": {
            "distance_3": run_task_b(google_rows, 3, predictions),
            "distance_5": run_task_b(google_rows, 5, predictions),
        },
        "task_c": run_task_c(google_rows, predictions),
    }

    pq.write_table(
        pa.Table.from_pylist(predictions, schema=PRED_SCHEMA),
        RESULTS_DIR / "predictions.parquet",
        compression="zstd",
    )
    write_json("metrics.json", metrics)

    write_json("run.json", {
        "model_run_id": model_run_id,
        "data_release": {
            "release_name": "quantum-data-core",
            "bundle_version": 3,
            "release_date": "2026-08-05",
        },
        "input_hashes": {
            SYNDROME_OBJECT: hashlib.sha256(lake_bytes(SYNDROME_OBJECT, settings)).hexdigest(),
            GOOGLE_OBJECT: hashlib.sha256(lake_bytes(GOOGLE_OBJECT, settings)).hexdigest(),
        },
        "code_revision": code_revision(),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in ("numpy", "pyarrow", "scikit-learn", "pandas")
        },
        "random_seed": SEED,
        "feature_order": {
            "task_a": [f"syndrome_bit_{i}" for i in range(16)],
            "task_b": ["detector_event_density", *GOOGLE_META_PREDICTION_COLUMNS],
            "task_c": [f"detector_bit_{i}" for i in range(200)],
        },
        "split_rules": {
            "task_a": "0.0005 validation; 0.005 test; all other rates train",
            "task_b_and_c": (
                "odd shot_index test; even shot_index ending in 8 validation; "
                "remaining even shot_index train"
            ),
        },
        "total_seconds": float(perf_counter() - start),
    })
    write_report(metrics)

    result = StageResult(
        stage="train",
        run_id=model_run_id,
        input_count=syndrome_table.num_rows + google_table.num_rows,
        output_count=len(predictions),
    )
    result.finish()
    return result


if __name__ == "__main__":
    stage = run("manual")
    print(
        f"{stage.stage}: read {stage.input_count} ML rows and wrote "
        f"{stage.output_count} test predictions"
    )
