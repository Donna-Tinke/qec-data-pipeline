"""Required Part II model-training stage.

Consume the two required ML input tables produced by Part I through the
supplied model-input and partition helpers. Publish repeatable model files,
predictions, metrics, run settings, the exact feature order, and the concise
Part II report under ``results/part2/``. Numerical performance is not graded.
"""

from quantum_lake_student.models import StageResult


def run(model_run_id: str) -> StageResult:
    raise NotImplementedError("Implement the documented QEC decoder training stage")
