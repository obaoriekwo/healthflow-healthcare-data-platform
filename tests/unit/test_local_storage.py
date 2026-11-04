import pytest

from ingestion.storage import ObjectExistsError


def test_bronze_objects_cannot_be_overwritten(storage, tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("x")
    storage.upload_file(src, "bronze/encounters/a.csv")
    with pytest.raises(ObjectExistsError):
        storage.upload_file(src, "bronze/encounters/a.csv")


def test_round_trip_and_list(storage, tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("hello")
    storage.upload_file(src, "bronze/encounters/d=1/a.csv")
    storage.download_file("bronze/encounters/d=1/a.csv", tmp_path / "out.csv")
    assert (tmp_path / "out.csv").read_text() == "hello"
    assert storage.list("bronze/encounters/") == ["bronze/encounters/d=1/a.csv"]


def test_keys_cannot_escape_the_root(storage, tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("x")
    with pytest.raises(ValueError):
        storage.upload_file(src, "../outside.csv")
