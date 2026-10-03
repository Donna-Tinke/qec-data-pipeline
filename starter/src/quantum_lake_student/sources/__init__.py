"""Dataset sources for the QEC data lake."""

from quantum_lake_student.sources import qasmbench_analysis, qasmbench_gold, qasmbench_silver

# Backward-compatibility alias
qasmbench = qasmbench_silver

__all__ = [
    "qasmbench",
    "qasmbench_silver",
    "qasmbench_gold",
    "qasmbench_analysis",
]
