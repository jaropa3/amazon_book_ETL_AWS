import logging
import os
import sys
from datetime import datetime
from pathlib import Path

_FORMATTER = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup_logger(name: str) -> logging.Logger:
    """Logger piszący na stdout ORAZ (poza Lambdą) do dziennego pliku logs/etl_<data>.log.

    Plik ma stabilną nazwę dzienną (nie per-wywołanie) i `delay=True`, więc jest odporny na
    wielokrotny import: parsowanie DAG-a nic nie zapisuje → plik nie powstaje (ang. lazy open),
    a taski runu dopisują do JEDNEGO pliku dnia — brak eksplozji pustych plików.

    Pod Airflow stdout łapie orkiestrator (airflow/logs/.../task_id=...), ale to logi rozbite
    per task w JSON. Dzienny plik dokłada czytelny, ciągły zapis całego przebiegu pipeline'u
    na dysku (ang. audit trail). Podsumowanie runu jest osobno w logs/pipeline_runs.log.

    Pod Lambdą kod leży w `/var/task`, które w runtime jest tylko do odczytu — próba zapisu
    tam wywala funkcję na starcie (`Read-only file system`), zanim jeszcze cokolwiek zaloguje.
    CloudWatch Logs i tak przechwytuje stdout, więc plik na dysku byłby tam też zwykłą
    duplikacją — file handler jest pomijany, gdy `AWS_LAMBDA_FUNCTION_NAME` (zmienna ustawiana
    automatycznie przez środowisko wykonawcze Lambdy) jest obecna.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    # Bez propagacji do roota (ang. logger hierarchy): pod Airflow root ma własne
    # handlery — inaczej każdy rekord byłby emitowany dwa razy (double logging).
    logger.propagate = False

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(_FORMATTER)
    logger.addHandler(stream_handler)

    if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
        log_dir = Path(__file__).resolve().parent / "logs"
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(
            log_dir / f"etl_{datetime.now():%Y-%m-%d}.log", delay=True, encoding="utf-8"
        )
        file_handler.setFormatter(_FORMATTER)
        logger.addHandler(file_handler)

    return logger
