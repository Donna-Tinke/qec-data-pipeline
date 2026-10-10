"""Dataset sources for the QEC data lake."""

from quantum_lake_student.sources import (
    google_qec,
    qasmbench_silver,
    qec_syndromes,
)

qasmbench = qasmbench_silver

__all__ = [
    "google_qec",
    "qec_syndromes",
    "qasmbench",
    "qasmbench_silver",
]
