import json
from spark_skew_lab.generator import make_events, write_jsonl


def test_generator_is_deterministic():
    assert make_events(20, seed=42) == make_events(20, seed=42)


def test_generator_empty_and_write(tmp_path):
    path = tmp_path / "empty.jsonl"
    assert write_jsonl([], path) == 0
    assert path.read_text() == ""


def test_generator_rejects_bad_parameters():
    import pytest
    with pytest.raises(ValueError):
        make_events(-1)
    with pytest.raises(ValueError):
        make_events(1, hot_share=1.5)
