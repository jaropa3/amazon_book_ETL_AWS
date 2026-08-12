# amazon_books_ETL_AWS

![tests](https://github.com/jaropa3/amazon_book_ETL_AWS/actions/workflows/tests.yml/badge.svg)

Serverless pipeline ETL na AWS: scrapuje książki z Amazona, ląduje je w S3, transformuje przez
dbt na Athena (Trino) w architekturze warstwowej (bronze → staging → intermediate → marts), a
całość orkiestruje Step Functions na godzinnym harmonogramie (EventBridge). To migracja
[wersji on-prem](https://github.com/jaropa3/amazon_book_ETL) (Airflow + PostgreSQL) — v1 jest
zamrożona jako osobny artefakt portfolio, ten projekt to niezależny, w pełni chmurowy przepis
tego samego problemu.

## Stack

**Python 3.14** · **AWS Lambda** · **S3** · **Glue Data Catalog** · **Athena (Trino)** · **dbt** ·
**Docker + ECS Fargate** · **Step Functions** · **EventBridge** · **pytest** · **GitHub Actions (CI)**

## Architektura

```
Lambda (scraper) → S3 raw/dt=YYYY-MM-DD/
        ↓ (Step Functions, co godzinę)
MSCK REPAIR TABLE bronze.raw (dogrywa nowe partycje)
        ↓
ECS Fargate (kontener dbt) → dbt run --target athena: staging → intermediate → marts
        ↓
log przebiegu → S3 + Athena
```

📐 **Pełny diagram, przepływ danych i decyzje architektoniczne: [docs/architecture.md](docs/architecture.md)**

## Wymagania

- **Python 3.14** + [uv](https://docs.astral.sh/uv/)
- **Konto AWS** z skonfigurowanym profilem CLI (`aws configure --profile <nazwa>`) — do lokalnego
  uruchomienia scrapera/dbt przeciwko realnym zasobom
- **Docker** — tylko do budowania obrazu dbt pod ECS Fargate (`docker/Dockerfile`)

> [!note] Ten projekt to nie "sklonuj i odpal jednym poleceniem"
> To jest w pełni wdrożona infrastruktura chmurowa na koncie AWS autora (Lambda, S3, Glue, ECS,
> Step Functions, EventBridge) — świadomie stawiana ręcznie (CLI/konsola), nie przez Terraform/CDK.
> Sklonowanie repo daje Ci **kod** (scraper, modele dbt, Dockerfile) do przeczytania i uruchomienia
> lokalnie/na własnym koncie AWS — nie odtworzy automatycznie cudzej infrastruktury.

## Konfiguracja

**1. Zależności:**
```bash
uv sync
source .venv/bin/activate
```

**2. `.env`** — skopiuj [`.env.example`](.env.example) i wskaż własny profil AWS:
```bash
cp .env.example .env
```

**3. `src/config.yaml`** — parametry scrapera + `aws:` (bucket/region/prefix na **Twoim** koncie,
jeśli chcesz zapisywać do własnego S3, nie autora).

**4. Profil dbt** — `~/.dbt/profiles.yml`, klucz `amazon_books_etl_aws` z targetami `dev`
(DuckDB — świadomie nieutrzymywany, patrz Decisions) i `athena` (wymaga własnego Glue Data
Catalog + bucketu wyników Athena).

## Uruchomienie

```bash
# scraper — lokalnie, zapisuje do S3
python src/main.py

# dbt lokalnie, na realnej Athenie (jeśli masz skonfigurowany target athena)
dbt run --project-dir dbt_project --profiles-dir ~/.dbt --target athena

# cały pipeline w chmurze, ręcznie (poza harmonogramem co godzinę)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:eu-central-1:<account-id>:stateMachine:amazon-books-pipeline \
  --input "{}" --profile <twój-profil>
```

Build i deploy obrazu dbt pod ECS Fargate:
```bash
docker build -f docker/Dockerfile -t dbt-runner .
docker tag dbt-runner:latest <account-id>.dkr.ecr.eu-central-1.amazonaws.com/dbt-runner:latest
docker push <account-id>.dkr.ecr.eu-central-1.amazonaws.com/dbt-runner:latest
```

## Testy i CI

```bash
pytest        # testy kodu — scraper, config, bez sieci/AWS
ruff check .
```

Testy uruchamiają się automatycznie po każdym push/PR ([.github/workflows/tests.yml](.github/workflows/tests.yml)).
Testy jakości danych są osobno, w warstwie dbt (`dbt_project/tests/`, `schema.yml`) i odpalają się
co godzinę wraz z pipeline'em, nie w CI.

## Decisions & trade-offs

- **Cała baza i cały projekt w chmurze — zero lokalnego Postgresa jako mostku.** v1 (Postgres,
  ten sam fizyczny serwer co produkcja) jest zamrożona; ten projekt świadomie nie współdzieli z
  nią żadnego stanu, nawet tymczasowo.
- **`MSCK REPAIR TABLE`, nie Glue Crawler, do bieżącego dogrywania partycji.** Crawler przy
  każdym pełnym rekrawlu tej samej wielopartycyjnej tabeli potrafił rozbić ją na osobne tabele
  per partycja — powtórzyło się dwukrotnie mimo identycznego schematu. Crawler zostaje jako
  narzędzie **jednorazowe** (ustalenie schematu nowej tabeli), nie krok pipeline'u. Koszt: jeśli
  format źródłowych danych się zmieni, trzeba świadomie odpalić crawler ręcznie, nie stanie się
  to samo.
- **dbt jako kontener na ECS Fargate, nie Lambda ani Glue Python Shell.** Zweryfikowane
  wyszukiwaniem, nie tylko intuicją: to najczęściej dokumentowany wzorzec społeczności dla
  dbt-core w produkcji na AWS. Lambda ma twardy limit 15 min i koncepcyjnie nie pasuje do ETL
  (rośnie z czasem, w przeciwieństwie do scrapera o stałym rozmiarze); Glue Python Shell nie miał
  realnego pokrycia w praktyce, mimo że technicznie działa.
- **dbt Core 1.12 (Python) + `dbt-athena-community`, migracja na Fusion / dbt Core v2 świadomie
  odłożona.** Fusion to przepisany w Rust silnik dbt (statyczna analiza SQL przed wysłaniem do
  hurtowni, lineage na poziomie kolumn); od czerwca 2026 jego fundament jest też podstawą dbt Core
  v2.0 na Apache 2.0. Przejście jest tu dziś **niemożliwe technicznie**, nie tylko niepriorytetowe:
  Fusion wymaga adaptera napisanego w Rust (połączenie przez ADBC), a adapter dla Atheny nie
  istnieje — [issue #829](https://github.com/dbt-labs/dbt-fusion/issues/829) jest otwarte od
  2025-09-26 bez przypisanej osoby, milestone'u i daty. To realny koszt wyboru niszowego silnika
  (Athena/Trino) zamiast Snowflake/BigQuery/Databricks, które wsparcie dostały pierwsze: **wolniejszy
  dostęp do nowości w ekosystemie**. Świadomie nie optymalizuję dziś projektu „pod Fusion", bo
  byłoby to strojenie pod hipotezę; wersja 1.12 jest jednocześnie rekomendowanym przystankiem na
  ścieżce migracji do v2, więc przejście nie będzie wymagało przepisywania modeli.
- **Step Functions + EventBridge, nie MWAA (zarządzany Airflow).** MWAA rozlicza się za
  **istnienie** środowiska (~$0.49/h, 24/7), nie za wykonanie — nieproporcjonalny koszt dla
  rzadkiego, godzinowego pipeline'u. Koszt: mniej "gotowej z pudełka" obserwowalności niż Airflow
  (stąd własny log przebiegu, patrz niżej), i mniej ekspresyjny język przepływu (ASL/JSONata) niż
  Python DAG-a.
- **Log przebiegu do S3 + Athena, nie DynamoDB.** Zero nowej usługi, spójność z resztą stacku —
  wszystko zapytywalne tym samym SQL-em. Koszt: odczyt statusu ostatniego runu to sekundy (start
  zapytania Athena), nie milisekundy jak przy KV store — nieistotne przy sporadycznym sprawdzaniu.
- **Zero lokalnego file handlingu — brak wzorca `raw/` → `processed/` z v1.** W v1 idempotencję
  zapewniało fizyczne przenoszenie skonsumowanych plików CSV (`shutil.move` do
  `processed/<data>/`, FIFO po najstarszym pliku). Tutaj zastąpiło to **partycjonowanie po dacie
  w S3** (`raw/dt=YYYY-MM-DD/`) + idempotentny merge w dbt po kluczu biznesowym: plik zostaje
  tam, gdzie wylądował, a "przetworzony" nie jest stanem na dysku, tylko wynikiem
  deterministycznej transformacji. Scraper nie dotyka lokalnego dysku w ogóle — CSV powstaje w
  pamięci (`io.StringIO`) i idzie prosto do S3, bo pod Lambdą `/var/task` jest read-only. Koszt:
  nie da się na pierwszy rzut oka (`ls`) powiedzieć, które partycje już przeszły przez pipeline —
  odpowiada na to dopiero log przebiegu w Athenie.
- **Brak filtra czasowego w modelach incremental (`fct_books_history`, `rejected_books`).**
  Każdy `dbt run` skanuje całe `bronze.raw` i robi idempotentny merge po kluczu biznesowym —
  spóźniona partycja (np. po awarii wcześniejszego przebiegu) zostaje złapana automatycznie przy
  najbliższym udanym runie. To ta sama decyzja co w v1, tylko bez jawnego mechanizmu FIFO/backlog
  — ochrona jest wpisana w sposób budowania modelu, nie w osobny krok wykrywania zaległości.
- **Niepełny scrape jest akceptowany jako sukces, nie traktowany jako błąd.** Amazon odpowiada
  `503` falami, więc przebieg regularnie zbiera 1–5 stron z 5 (widać to w `scraped_count` w logu
  przebiegu: od ~20 do ~62 rekordów). Rozważaną alternatywą był próg — „mniej niż X% stron =
  rzuć wyjątek" — i **świadomie go nie wdrażam**: przy źródle, którego dostępność jest poza
  kontrolą pipeline'u, taki próg zamienia ograniczenie źródła w kaskadę fałszywych alarmów, a
  ponowienie i tak trafia w tę samą falę blokady. Koszt jest realny: partycja dobowa bywa
  niekompletna, więc **te dane nadają się do analizy trendów, nie do twierdzeń o pokryciu
  katalogu**. Wybrana ochrona jest inna niż blokowanie: godzinowy harmonogram sprawia, że
  brakujące pozycje łapie kolejny przebieg, a merge po kluczu biznesowym nie duplikuje tych
  już zebranych.

### Co bym poprawił

- **Infrastruktura tylko w CLI/konsoli, bez Terraform/CDK** — świadoma decyzja na czas nauki, ale
  odtworzenie tego środowiska od zera dziś wymaga pamiętania ~20 komend z historii, nie jednego
  `terraform apply`.
- **Brak alertów (Slack) przy nieudanym przebiegu** — dziś jedyny sposób, żeby dowiedzieć się o
  porażce, to ręcznie sprawdzić konsolę Step Functions.
- **Region (`eu-central-1`) i ARN-y konta wpisane wprost w kod/definicje**, nie parametryzowane —
  wystarczające dla jednego środowiska, nie przenośne bez ręcznej podmiany.
- **`MSCK REPAIR` skanuje cały prefiks S3 co przebieg** — przy realnym wzroście liczby partycji
  (setki/tysiące dni) warto rozważyć Partition Projection w Athenie (deklaratywne partycje bez
  rejestrowania każdej osobno w katalogu) zamiast skanowania.

## Dokumentacja

- [Architektura](docs/architecture.md)
- [Notatki z sesji nauki](docs/notes/) — koncepty AWS/data engineering poznane przy budowie tego projektu
