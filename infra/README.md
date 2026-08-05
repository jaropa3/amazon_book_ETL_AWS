# infra/ — zrzuty stanu infrastruktury (ang. state snapshots)

Zasoby AWS w tym projekcie są tworzone **z konsoli**, świadomie — celem nauki jest zrozumienie
usług, nie nauka narzędzia IaC. Ten katalog nie jest więc źródłem prawdy (ang. source of truth):
prawdą jest to, co stoi w AWS. To są **zrzuty stanu**, żeby zmiany w orkiestracji zostawiały ślad
w historii gita — inaczej za miesiąc nie da się odpowiedzieć na pytanie „kiedy i po co pojawił się
ten retry".

Odświeżenie po każdej zmianie w konsoli:

```bash
./scripts/dump_infra.sh
git diff infra/
```

## Zawartość

| Plik | Zasób |
|---|---|
| `stepfunctions-amazon-books-pipeline.json` | definicja state machine `amazon-books-pipeline` |
| `eventbridge-amazon-books-schedule.json` | reguła harmonogramu (`rate(1 hour)`) |
| `ecs-taskdef-dbt-runner.json` | task definition `dbt-runner` (Fargate) |
| `iam-ecs-task-role-policy.json` | uprawnienia roli zadania ECS (S3 + Glue + Athena) |
| `iam-ecs-task-trust-policy.json` | trust policy tej roli — `aws:SourceArn` zawęża ją do `eu-central-1` |

Dwa ostatnie pliki leżały wcześniej w `docker/`, choć ani `docker build`, ani runtime kontenera
ich nie czyta — to dokumenty polityk wklejane w konsoli IAM. Były pisane ręcznie, więc mogły
rozjechać się z tym, co faktycznie wisi na roli; teraz są zrzucane z AWS jak reszta.

## Trade-off

**Zysk:** zmiany infrastruktury widać w `git diff` i w code review; można wrócić do poprzedniej
wersji definicji.

**Koszt:** zrzut jest **jednokierunkowy** (AWS → repo). Nie da się z niego odtworzyć środowiska,
a zmiana klikniętena w konsoli i nieodświeżona tutaj sprawia, że repo cicho kłamie. Dryf
(ang. drift) nie jest wykrywany automatycznie — `dump_infra.sh` trzeba uruchomić ręcznie.

**Docelowo:** Terraform (`aws_sfn_state_machine`, `aws_cloudwatch_event_rule`,
`aws_ecs_task_definition`) daje kierunek repo → AWS i wykrywa dryf przez `terraform plan`.
Świadoma decyzja: **wdrożenie odłożone do następnego projektu** — tutaj koszt nauki HCL-a
przewyższyłby zysk przy kilku zasobach zmienianych raz na tydzień.
