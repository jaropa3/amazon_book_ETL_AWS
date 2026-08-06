"""Kompakcja strefy lądowania (ang. raw landing zone): wiele małych CSV → jeden plik na partycję.

Glue czyta `bronze.raw` z `skip.header.line.count=1`, co pomija pierwszą linię **każdego pliku
osobno**. Zwykłe sklejenie N plików przepuściłoby więc N-1 wierszy nagłówka do danych (asin='asin')
— i to jest jedyny powód, dla którego ten moduł parsuje CSV zamiast łączyć bajty.
"""

from collections.abc import Sequence
from datetime import date, datetime, timezone

import boto3

from config import CONFIG
from logger import setup_logger

logger = setup_logger("Amazon_books_ETL")

COMPACTED_FILENAME = "part-0000.csv"
# S3 kasuje maksymalnie 1000 kluczy na wywołanie; partycja ma ich ~20, więc jedna paczka wystarcza.
DELETE_BATCH_SIZE = 1000


class SchemaMismatchError(Exception):
    """Pliki w jednej partycji mają różne nagłówki — scalenie zmieniłoby znaczenie kolumn."""


def merge_csv_parts(parts: Sequence[str]) -> str:
    """Scala dokumenty CSV o wspólnym nagłówku w jeden, z dokładnie jednym nagłówkiem.

    Czysta i deterministyczna: bez I/O, kolejność wierszy zachowana zgodnie z kolejnością `parts`.
    Podnosi SchemaMismatchError, gdy nagłówki się różnią — cicha zmiana schematu w warstwie raw
    jest gorsza niż zatrzymanie kompakcji.
    """
    header: str | None = None
    rows: list[str] = []

    for part in parts:
        lines = [line for line in part.splitlines() if line.strip()]
        if not lines:
            continue
        if header is None:
            header = lines[0]
        elif lines[0] != header:
            raise SchemaMismatchError(f"nagłówek {lines[0]!r} != {header!r}")
        rows.extend(lines[1:])

    if header is None:
        return ""
    return "\n".join([header, *rows]) + "\n"


def partition_date_of(key: str, raw_prefix: str) -> str | None:
    """Wyciąga wartość partycji z klucza `…/raw/dt=YYYY-MM-DD/plik.csv`."""
    remainder = key[len(raw_prefix):] if key.startswith(raw_prefix) else key
    head, _, _ = remainder.partition("/")
    if not head.startswith("dt="):
        return None
    return head[len("dt="):]


def list_partition_keys(s3, bucket: str, raw_prefix: str) -> dict[str, list[str]]:
    """Mapuje datę partycji → posortowane klucze plików CSV. Sortowanie po nazwie daje
    porządek chronologiczny, bo nazwy zawierają znacznik czasu sesji."""
    partitions: dict[str, list[str]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=raw_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            partition = partition_date_of(key, raw_prefix)
            if partition is not None:
                partitions.setdefault(partition, []).append(key)
    for keys in partitions.values():
        keys.sort()
    return partitions


def select_compactable(partitions: dict[str, list[str]], today: date) -> dict[str, list[str]]:
    """Tylko partycje **zamknięte** (starsze niż dziś) i mające co scalać.

    Bieżącej partycji nie ruszamy — scraper wciąż do niej dopisuje, a kompakcja w trakcie
    zapisu zgubiłaby pliki, które powstały między listowaniem a kasowaniem.
    """
    return {
        partition: keys
        for partition, keys in partitions.items()
        if len(keys) > 1 and partition < today.isoformat()
    }


def _delete_keys(s3, bucket: str, keys: Sequence[str]) -> None:
    """Kasuje po jawnej liście kluczy (nigdy po prefiksie) i traktuje częściową porażkę
    jako błąd — pozostawione oryginały obok pliku scalonego to duplikaty w `bronze.raw`."""
    for start in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[start:start + DELETE_BATCH_SIZE]
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"nie udało się skasować {len(errors)} obiektów: {errors[:3]}")


def compact_partition(s3, bucket: str, raw_prefix: str, partition: str, keys: Sequence[str]) -> dict:
    """Scala jedną partycję. Kolejność jest istotna: zapis pliku scalonego → weryfikacja →
    dopiero potem kasowanie oryginałów. Odwrotna kolejność oznaczałaby okno utraty danych.

    Idempotentna: istniejący `part-0000.csv` jest jednym ze źródeł, więc ponowne uruchomienie
    na już scalonej partycji przepisuje ten sam plik i nie kasuje niczego.
    """
    target_key = f"{raw_prefix}dt={partition}/{COMPACTED_FILENAME}"

    parts = [
        s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for key in keys
    ]
    merged = merge_csv_parts(parts)
    row_count = max(len(merged.splitlines()) - 1, 0)

    s3.put_object(Bucket=bucket, Key=target_key, Body=merged.encode("utf-8"))

    stale_keys = [key for key in keys if key != target_key]
    _delete_keys(s3, bucket, stale_keys)

    logger.info(
        "kompakcja dt=%s: %d plików → 1 (%d wierszy, %d B)",
        partition, len(keys), row_count, len(merged.encode("utf-8")),
    )
    return {
        "partition": partition,
        "files_before": len(keys),
        "files_after": 1,
        "row_count": row_count,
    }


def compact_raw(today: date | None = None) -> dict:
    """Brzeg systemu (ang. edge): kompaktuje wszystkie zamknięte partycje warstwy raw."""
    today = today or datetime.now(timezone.utc).date()
    s3 = boto3.client("s3", region_name=CONFIG.aws.region)

    partitions = list_partition_keys(s3, CONFIG.aws.bucket, CONFIG.aws.raw_prefix)
    compactable = select_compactable(partitions, today)

    if not compactable:
        logger.info("kompakcja: brak partycji do scalenia")
        return {"partitions_compacted": 0, "files_removed": 0, "details": []}

    details = [
        compact_partition(s3, CONFIG.aws.bucket, CONFIG.aws.raw_prefix, partition, keys)
        for partition, keys in sorted(compactable.items())
    ]
    files_removed = sum(d["files_before"] - d["files_after"] for d in details)
    logger.info(
        "kompakcja zakończona: %d partycji, %d plików mniej",
        len(details), files_removed,
    )
    return {
        "partitions_compacted": len(details),
        "files_removed": files_removed,
        "details": details,
    }
