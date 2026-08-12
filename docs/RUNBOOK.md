# RUNBOOK

Instrukcja operacyjna (ang. *operations runbook*): **co zrobić, gdy pipeline się zepsuł**.

Odbiorca to dyżurny (ang. *on-call*) — ktoś, kto tego systemu nie budował, patrzy na czerwony
przebieg o 3 w nocy i potrzebuje komendy, nie wykładu o architekturze.

| Dokument | Odpowiada na pytanie |
|---|---|
| [README.md](README.md) | co to jest, jak uruchomić |
| [docs/architecture.md](docs/architecture.md) | jak działa i dlaczego tak |
| **RUNBOOK.md** (ten plik) | **co zrobić, gdy padło** |

> [!important] Zasada zero
> **Pojedynczy nieudany przebieg zwykle NIE wymaga reakcji.** Harmonogram to `rate(1 hour)`, a modele
> dbt nie mają znacznika wodnego (ang. *watermark*) — każdy przebieg skanuje całe `bronze.raw` i robi
> idempotentny merge po `book_sk`. Następny udany przebieg nadrabia zaległości sam. Reaguj, gdy
> **dwa lub więcej** przebiegów pod rząd są czerwone, albo gdy dane w `marts` nie odświeżyły się
> ponad 3 h.

---

## 0. Namiary (ang. *quick reference*)

| Co | Wartość |
|---|---|
| Konto / region | `915238109570` / `eu-central-1` (**zawsze** — role IAM mają `aws:SourceArn` zawężony do tego regionu) |
| Profil AWS CLI | `amazon-books-etl-dev` |
| State machine | `arn:aws:states:eu-central-1:915238109570:stateMachine:amazon-books-pipeline` |
| Harmonogram | EventBridge, reguła `amazon-books-schedule`, `rate(1 hour)` |
| Lambda | `amazon-books-scraper` (dwa tryby: domyślny = scrape, `{"mode":"compact"}` = kompakcja) |
| Kontener dbt | ECS Fargate, klaster `amazon-books-cluster`, task def `dbt-runner` |
| Bucket | `s3://amazon-books-etl-aws-915238109570` |
| Log group Lambdy | `/aws/lambda/amazon-books-scraper` |
| Log group dbt | `/ecs/dbt-runner` |
| Log przebiegów | `s3://amazon-books-etl-aws-915238109570/logs/pipeline_runs/dt=YYYY-MM-DD/<run_id>.json` |

Kroki state machine, w kolejności:
`InitRunVars → Lambda Invoke → GetPartitionsBefore → MsckRepair → CompactRaw → GetPartitionsAfter
→ ECS RunTask → LogRun → CheckRunStatus → (SucceedRun | FailRun)`

---

## 1. Health check — czy to w ogóle żyje

Kolejność od najtańszej diagnozy do najdroższej.

**1.1. Ostatnie 10 przebiegów:**
```bash
./scripts/last_executions.sh
```
Szukasz: `Status = SUCCEEDED` i `Start` nie starszy niż ~1 h.

**1.2. Który krok padł (gdy status = FAILED):**
```bash
aws stepfunctions get-execution-history \
  --execution-arn <ARN_Z_PUNKTU_1.1> \
  --profile amazon-books-etl-dev --region eu-central-1 \
  --query 'events[?type==`ExecutionFailed` || contains(type, `TaskFailed`)]' --output json
```
Nazwa kroku z tego wyniku prowadzi wprost do sekcji 2.

**1.3. Logi scrapera (Lambda):**
```bash
./scripts/my_AWS_logs.sh
```

**1.4. Logi dbt (ECS):**
```bash
aws logs tail /ecs/dbt-runner --since 3h \
  --profile amazon-books-etl-dev --region eu-central-1
```

**1.5. Czy dane faktycznie doszły** (najtwardszy dowód — reszta może kłamać):
```sql
-- Athena, workgroup domyślny. Filtr po kolumnie partycyjnej, konkretne kolumny, nie SELECT *.
SELECT dt, count(*) AS rows
FROM bronze.raw
WHERE dt >= date_format(current_date - interval '2' day, '%Y-%m-%d')
GROUP BY dt ORDER BY dt DESC;
```

**1.6. Log przebiegów w S3** (zawiera też przebiegi nieudane — `LogRun` ma odpowiednik
airflowowego `trigger_rule=ALL_DONE`, każdy `Catch` prowadzi do niego):
```bash
aws s3 ls s3://amazon-books-etl-aws-915238109570/logs/pipeline_runs/ --recursive \
  --profile amazon-books-etl-dev | tail -5
```
Pola w JSON-ie: `status`, `error`, `compaction_error`, `scraped_count`,
`partitions_before/after/added`, `backlog_recovered`.

---

## 2. Znane awarie (ang. *known failure modes*)

Format: **symptom → jak potwierdzić → jak naprawić**.

### 2.1. `ECS RunTask` FAILED — czyli padł `dbt run`

To najczęstsza awaria: krok **nie ma `Retry`**, tylko `Catch`, więc jedna wywrotka dbt = czerwony
cały przebieg.

- **Potwierdź:** `aws logs tail /ecs/dbt-runner --since 1h` → szukaj `Database Error` /
  `Compilation Error` / `Runtime Error`.
- **Napraw:** zwykle nic — poczekaj na następny przebieg (patrz Zasada zero). Jeśli błąd jest
  deterministyczny (powtarza się co godzinę), to bug w modelu — sekcja 2.6.
- **Wymuś natychmiast:** sekcja 3.1.

> [!warning] Bezpieczeństwo powtórzenia (ang. *idempotency*)
> Powtórzenie całego przebiegu jest bezpieczne: scraper pisze nowy plik z timestampem, modele
> robią merge po `book_sk`. Jedyny efekt uboczny to dodatkowy koszt skanu Atheny.

### 2.2. `Lambda Invoke` FAILED — padł scraper

- **Potwierdź:** `./scripts/my_AWS_logs.sh` → timeout, błąd HTTP, zmiana HTML u Amazona.
- **Napraw:** timeout / 5xx / 429 → nic nie rób, następny przebieg za godzinę. Powtarzalny błąd
  parsowania → Amazon zmienił stronę, trzeba poprawić selektory w [src/main.py](src/main.py).
- **Uwaga:** scraper szuka autora po `href` zawierającym `/e/ASIN`, **nie po klasie CSS** — klasa
  łapała „Paperback". Jeśli „naprawiasz" to klasą CSS, cofasz starą poprawkę.

### 2.3. `status = succeeded`, ale książek mniej niż zwykle

**To nie jest awaria.** Częściowy scrape (ang. *partial scrape*) jest z założenia akceptowany —
Amazon bywa kapryśny, a pipeline ma dostarczyć to, co się dało. Nie ma progu kompletności i nie
dodawaj go.

Reaguj tylko, gdy `scraped_count = 0` **kilka przebiegów pod rząd** → sekcja 2.2.

### 2.4. `bronze.raw` zniknęła / rozbiła się na `dt_2026_07_23`, `dt_2026_07_24`…

Klasyczna, **znana zawodność Glue Crawlera** przy pełnym rekrawlu (`CRAWL_EVERYTHING`) — zdarzyło
się dwukrotnie (22-07 i 24-07) mimo spójnego schematu i SerDe.

- **Potwierdź:** `aws glue get-tables --database-name bronze --profile amazon-books-etl-dev
  --region eu-central-1 --query 'TableList[].Name'` → widzisz tabele per partycja.
- **Napraw:** usuń tabele-śmieci, przywróć jedną `raw` (6 kolumn, `string`, `OpenCSVSerde`),
  potem `MSCK REPAIR TABLE bronze.raw`.
- **Zapobiegaj:** crawler jest **wyłączony z pipeline'u na stałe**. Patrz sekcja 4.

### 2.5. `MsckRepair` FAILED (Athena)

- **Potwierdź:** `aws athena get-query-execution --query-execution-id <ID>` → `Status.StateChangeReason`.
- **Typowe przyczyny:** brak `bronze.raw` (→ 2.4), brak uprawnień do S3 dla roli
  `amazon-books-pipeline-role`, pełny bucket wyników Atheny.
- **Skutek:** nowa partycja nie zostanie zarejestrowana, ale **dane nie giną** — leżą w S3.
  Następny udany `MSCK REPAIR` dogra je wstecz (`backlog_recovered: true` w logu przebiegu).

### 2.6. `dbt` sypie `CAST('' AS DOUBLE)` albo inny błąd Trino

Trino — w przeciwieństwie do DuckDB — **rzuca błędem** na pustym stringu, nie zwraca cicho `NULL`.
Każde nowe czyszczenie liczby musi przejść przez `nullif(..., '')` **przed** castem.

- **Uwaga:** target `dev` (DuckDB) jest **świadomie nieutrzymywany**. Jeśli model działa na Athenie,
  a wywala się na DuckDB — to nie jest bug do naprawy.

### 2.7. `AccessDenied` / `ecs:RunTask is not authorized` po zmianie roli w konsoli

- **Najczęstsza przyczyna: zły region.** IAM jest globalne, ale trust policy ról
  (`ecsTaskExecutionRole`, `ECS-role-db03eec9`) ma warunek `aws:SourceArn` zawężony do
  `eu-central-1`. Rola albo zasób utworzony w innym regionie **nie zadziała**, mimo że w konsoli
  wygląda poprawnie.
- **Diagnoza:** IAM Policy Simulator **od razu**, nie po godzinie czytania logów.
- **Po naprawie:** `./scripts/dump_infra.sh` + `git diff infra/`.

### 2.8. `compaction_error` w logu, ale `status = succeeded`

To projekt świadomy: `CompactRaw` ma `Catch → GetPartitionsAfter`, więc **nieudana kompakcja nie
wywraca przebiegu** — dane są już w S3, kompakcja to tylko higiena małych plików (ang. *small files
problem*).

- **Napraw:** ignoruj pojedynczy przypadek. Jeśli powtarza się przez dobę → uruchom ręcznie:
  ```bash
  aws lambda invoke --function-name amazon-books-scraper \
    --payload '{"mode":"compact"}' --cli-binary-format raw-in-base64-out \
    --profile amazon-books-etl-dev --region eu-central-1 /dev/stdout
  ```

---

## 3. Procedury rutynowe

### 3.1. Ręczne uruchomienie pipeline'u
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:eu-central-1:915238109570:stateMachine:amazon-books-pipeline \
  --input "{}" --profile amazon-books-etl-dev --region eu-central-1
```

### 3.2. Dogranie zaległej partycji (ang. *backfill*)
Nie ma osobnej procedury — to **ten sam przebieg**. `MSCK REPAIR` dogra brakujące partycje, a modele
bez watermarka złapią spóźnione dane (ang. *late-arriving data*). Wystarczy 3.1.

### 3.3. Wstrzymanie harmonogramu (prace, kontrola kosztów)
```bash
aws events disable-rule --name amazon-books-schedule \
  --profile amazon-books-etl-dev --region eu-central-1
# ...i z powrotem:
aws events enable-rule  --name amazon-books-schedule \
  --profile amazon-books-etl-dev --region eu-central-1
```

### 3.4. Wdrożenie nowej wersji scrapera
```bash
./scripts/deploy_lambda.sh
```

### 3.5. Wdrożenie nowej wersji modeli dbt
```bash
docker build -f docker/Dockerfile -t dbt-runner .
docker tag dbt-runner:latest 915238109570.dkr.ecr.eu-central-1.amazonaws.com/dbt-runner:latest
docker push 915238109570.dkr.ecr.eu-central-1.amazonaws.com/dbt-runner:latest
```
Task definition wskazuje na tag `:latest`, więc następny przebieg weźmie nowy obraz — **nie trzeba**
rejestrować nowej rewizji.

### 3.6. Po każdej zmianie w konsoli AWS
```bash
./scripts/dump_infra.sh && git diff infra/
```
Snapshot infrastruktury w repo to proteza Terraforma — dopóki go nie ma, to jedyny zapis tego, jak
zasób naprawdę wygląda.

---

## 4. Czego NIE robić (ang. *destructive operations*)

> [!danger] Operacje nieodwracalne
> - **`dbt run --full-refresh` na `fct_books_history`** — model akumuluje realną historię od 22-07.
>   Full refresh = **bezpowrotna utrata historii**. Nie ma z czego odtworzyć: `bronze.raw` zawiera
>   tylko stany, nie przebieg zmian.
> - **`StartCrawler` w regularnym przebiegu** — patrz 2.4. Schemat `bronze.raw` jest ustalony;
>   crawler zostaje wyłącznie narzędziem jednorazowym do nowej tabeli od zera.
> - **Dodanie lokalnego Postgresa / profilu `amazon_books_etl` (bez `_aws`)** — to profil **v1**.
>   `dbt run` z tym profilem pisałby do zamrożonej historii gold v1.
> - **`DROP TABLE` na modelu z downstream** — przeładowuj dane, nie strukturę.

---

## 5. Eskalacja i koszty

Projekt jednoosobowy — ścieżka eskalacji (ang. *escalation path*) to Ty. Sensowny zamiennik
alarmu:

- **Billing alert** — pierwsza linia obrony przed pętlą, która się zapętliła. Nierosnący rachunek
  jest lepszym monitoringiem niż brak monitoringu.
- **Najdroższy element przy awarii:** Athena skanuje całe `bronze.raw` przy każdym przebiegu, a
  przebiegów jest 24/dobę. Zapętlony retry to koszt skanu × liczba prób.
- **Gdzie patrzeć:** Cost Explorer, grupowanie po tagu `Project`.

---

## 6. Automation backlog

Runbook to **lista długu automatyzacyjnego**, nie wieczna dokumentacja. Każdy wpis wykonany ręcznie
po raz trzeci powinien zostać skryptem w `scripts/` albo krokiem w state machine.

Kandydaci na dziś:

- [ ] **Alarm na dwa nieudane przebiegi pod rząd** — CloudWatch Alarm na metryce
      `ExecutionsFailed` state machine → SNS → mail. Dziś awarię wykrywa się, tylko patrząc.
- [ ] **Tabela Athena nad `logs/pipeline_runs/`** — log jest pisany jako JSON, ale nie ma nad nim
      tabeli, więc pytanie „ile przebiegów padło w tym tygodniu" wymaga `aws s3 ls` i czytania
      plików. Zewnętrzna tabela + partycja po `dt` domyka pętlę obserwowalności.
- [ ] **`scripts/health_check.sh`** — punkty 1.1–1.5 tej sekcji jako jedna komenda.
- [ ] **Terraform** — cała infrastruktura powstała w konsoli, `infra/*.json` to tylko snapshoty
      po fakcie. Odtworzenie środowiska od zera jest dziś procedurą ręczną, nie `terraform apply`.