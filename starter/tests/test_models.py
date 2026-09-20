from quantum_lake_student.models import StageResult, stable_record_hash


def test_record_hash_is_independent_of_dictionary_order() -> None:
    assert stable_record_hash({"a": 1, "b": [2, 3]}) == stable_record_hash(
        {"b": [2, 3], "a": 1}
    )


def test_stage_result_records_completion_time() -> None:
    result = StageResult(stage="test", run_id="run-1")
    result.finish()
    assert result.finished_at is not None
    assert result.finished_at >= result.started_at

