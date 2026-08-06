"""Testy kompakcji warstwy raw — bez S3, na atrapie klienta boto3."""

from datetime import date

import pytest

from compaction import (
    COMPACTED_FILENAME,
    SchemaMismatchError,
    compact_partition,
    merge_csv_parts,
    partition_date_of,
    select_compactable,
)

HEADER = "asin,title,author,price,rating,scraped_at"


def csv_doc(*rows: str) -> str:
    return "\n".join([HEADER, *rows]) + "\n"


class FakeS3:
    """Atrapa klienta S3 — tylko trzy operacje używane przez kompakcję."""

    def __init__(self, objects: dict[str, str]):
        self.objects = dict(objects)
        self.delete_errors: list[dict] = []

    def get_object(self, Bucket: str, Key: str):  # noqa: N803 - podpis boto3
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes):  # noqa: N803
        self.objects[Key] = Body.decode("utf-8")

    def delete_objects(self, Bucket: str, Delete: dict):  # noqa: N803
        if self.delete_errors:
            return {"Errors": self.delete_errors}
        for obj in Delete["Objects"]:
            self.objects.pop(obj["Key"], None)
        return {}


class _Body:
    def __init__(self, text: str):
        self.text = text

    def read(self) -> bytes:
        return self.text.encode("utf-8")


class TestMergeCsvParts:
    def test_keeps_exactly_one_header(self):
        merged = merge_csv_parts([csv_doc("a,1"), csv_doc("b,2"), csv_doc("c,3")])
        assert merged.splitlines().count(HEADER) == 1

    def test_keeps_every_data_row_in_order(self):
        merged = merge_csv_parts([csv_doc("a,1", "b,2"), csv_doc("c,3")])
        assert merged.splitlines()[1:] == ["a,1", "b,2", "c,3"]

    def test_rejects_mismatched_header(self):
        with pytest.raises(SchemaMismatchError):
            merge_csv_parts([csv_doc("a,1"), "inny,naglowek\nx,2\n"])

    def test_ignores_empty_parts(self):
        assert merge_csv_parts([csv_doc("a,1"), "", "   \n"]) == csv_doc("a,1")

    def test_empty_input_gives_empty_output(self):
        assert merge_csv_parts([]) == ""

    def test_header_only_files_produce_no_rows(self):
        assert merge_csv_parts([csv_doc(), csv_doc()]) == HEADER + "\n"


class TestPartitionDateOf:
    def test_extracts_partition(self):
        assert partition_date_of("raw/dt=2026-08-05/books_1.csv", "raw/") == "2026-08-05"

    def test_returns_none_outside_partition_layout(self):
        assert partition_date_of("raw/luzem.csv", "raw/") is None


class TestSelectCompactable:
    def test_skips_current_partition(self):
        partitions = {"2026-08-06": ["a", "b"], "2026-08-05": ["c", "d"]}
        selected = select_compactable(partitions, date(2026, 8, 6))
        assert set(selected) == {"2026-08-05"}

    def test_skips_partition_with_single_file(self):
        partitions = {"2026-08-04": ["only.csv"]}
        assert select_compactable(partitions, date(2026, 8, 6)) == {}


class TestCompactPartition:
    def _run(self, s3, keys):
        return compact_partition(s3, "bucket", "raw/", "2026-08-05", keys)

    def test_merges_into_single_object_and_removes_sources(self):
        keys = ["raw/dt=2026-08-05/books_1.csv", "raw/dt=2026-08-05/books_2.csv"]
        s3 = FakeS3({keys[0]: csv_doc("a,1"), keys[1]: csv_doc("b,2")})

        result = self._run(s3, keys)

        target = f"raw/dt=2026-08-05/{COMPACTED_FILENAME}"
        assert list(s3.objects) == [target]
        assert s3.objects[target] == csv_doc("a,1", "b,2")
        assert result == {
            "partition": "2026-08-05",
            "files_before": 2,
            "files_after": 1,
            "row_count": 2,
        }

    def test_is_idempotent(self):
        """Powtórzenie na już scalonej partycji nie zmienia zawartości ani nie gubi wierszy."""
        keys = ["raw/dt=2026-08-05/books_1.csv", "raw/dt=2026-08-05/books_2.csv"]
        s3 = FakeS3({keys[0]: csv_doc("a,1"), keys[1]: csv_doc("b,2")})
        self._run(s3, keys)

        target = f"raw/dt=2026-08-05/{COMPACTED_FILENAME}"
        before = dict(s3.objects)
        self._run(s3, [target])

        assert s3.objects == before

    def test_recovers_when_previous_run_died_before_deleting(self):
        """Plik scalony istnieje, oryginały zostały — ponowienie nie może zgubić wierszy."""
        target = f"raw/dt=2026-08-05/{COMPACTED_FILENAME}"
        leftover = "raw/dt=2026-08-05/books_2.csv"
        s3 = FakeS3({target: csv_doc("a,1"), leftover: csv_doc("b,2")})

        self._run(s3, [target, leftover])

        assert list(s3.objects) == [target]
        assert s3.objects[target] == csv_doc("a,1", "b,2")

    def test_raises_when_delete_partially_fails(self):
        """Pozostawione oryginały obok pliku scalonego = duplikaty w bronze.raw — to musi krzyczeć."""
        keys = ["raw/dt=2026-08-05/books_1.csv", "raw/dt=2026-08-05/books_2.csv"]
        s3 = FakeS3({keys[0]: csv_doc("a,1"), keys[1]: csv_doc("b,2")})
        s3.delete_errors = [{"Key": keys[0], "Code": "AccessDenied"}]

        with pytest.raises(RuntimeError, match="skasować"):
            self._run(s3, keys)

    def test_never_deletes_the_compacted_file_itself(self):
        target = f"raw/dt=2026-08-05/{COMPACTED_FILENAME}"
        s3 = FakeS3({target: csv_doc("a,1")})

        self._run(s3, [target])

        assert target in s3.objects
