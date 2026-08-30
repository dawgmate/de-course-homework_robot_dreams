# SPEC.md — специфікація домашнього завдання

Це головний документ ДЗ. Тут описано **що саме** має робити ваш DAG і за що нараховуються
бали. ETL-логіку писати **не треба** — готові функції завантаження/валідації/запису вже
дано в [`include/gh_etl.py`](include/gh_etl.py). Ваша робота — **оркестрація**: зібрати
DAG і custom sensor, що викликають ці функції в правильному порядку, за розкладом і
ідемпотентно.

Ви пишете рівно два файли (шляхи — від кореня `homework/`):

| Файл | Що це |
|---|---|
| `dags/github_archive_daily.py` | DAG |
| `plugins/gh_sensor.py` | custom `GHArchiveSensor` |

Стиль — на ваш вибір: класичний (`PythonOperator` + `xcom_push`/`xcom_pull`) або TaskFlow API
(`@dag`/`@task`). Критерії нижче сформульовані для класичного стилю; у TaskFlow еквівалент
`PythonOperator` — це `@task` (ім'я функції має збігатися з потрібним `task_id`), а XCom
передається аргументом функції.

## Що будуємо

Щоденний pipeline, що вантажить одну годину (14:00 UTC) подій **GitHub Archive** у DuckDB:

```
check_availability → download_archive → validate_file → load_to_duckdb → notify_completion
   (ваш sensor)        (PythonOperator)   (PythonOp.)       (PythonOp.)      (PythonOp.)
```

Готові цеглинки з `include/gh_etl.py` (імпортуйте, не переписуйте):

| Функція | Робить | Повертає |
|---|---|---|
| `download(ds, landing_dir)` | качає `{ds}-14.json.gz` у `landing_dir/<ds>/`, ідемпотентно | шлях до файлу |
| `validate(path)` | перевіряє розмір і структуру; кидає `ValueError` | — |
| `load_to_duckdb(path, ds, db)` | вантажить у `raw.github_events_raw`, перезаписуючи день `ds` | к-сть рядків |
| `summarize(ds, db)` | підсумок за день | `dict(rows, event_types)` |

Дано також (не чіпайте): `docker-compose.yml`, `Dockerfile`, `include/`.

Шляхи всередині контейнера:
```
DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"
```

---

## Завдання 1 — визначення DAG · 20 балів

- `dag_id = "github_archive_daily"` (5 б);
- розклад **щодня о 06:00 UTC** (`schedule="0 6 * * *"`) (7 б);
- `catchup=False` і коректний `start_date` (5 б);
- задано `tags` (для зручності пошуку в UI) (3 б).

---

## Завдання 2 — задачі і граф · 30 балів

- п'ять задач із саме такими `task_id`: `check_availability`, `download_archive`,
  `validate_file`, `load_to_duckdb`, `notify_completion` (10 б);
- `download_archive`/`validate_file`/`load_to_duckdb`/`notify_completion` —
  `PythonOperator`, що викликають відповідні функції з `gh_etl` (8 б);
- залежності у лінію: `check_availability >> download_archive >> validate_file >>
  load_to_duckdb >> notify_completion` (7 б);
- `download_archive` штовхає шлях до файлу в **XCom**, а `validate_file` і
  `load_to_duckdb` дістають його з XCom (а не качають повторно) (5 б).

---

## Завдання 3 — `GHArchiveSensor` · 25 балів

Custom sensor у `plugins/gh_sensor.py`:

- успадковує `BaseSensorOperator`, приймає параметр `hour` (8 б);
- `poke(context)` бере дату з `context["ds"]`, збирає URL
  `https://data.gharchive.org/<ds>-<hour>.json.gz`, робить **HTTP HEAD** і повертає
  `True` на 200, інакше `False` (12 б);
- у DAG доданий **першою** задачею з `timeout=600`, `poke_interval=60`,
  `mode="reschedule"` (5 б).

---

## Завдання 4 — ідемпотентність через logical date · 15 балів

- усі задачі працюють із `{{ ds }}` / `context["ds"]` (logical date), **не**
  `datetime.now()` (8 б);
- як наслідок — повторний прогін того самого дня **не дублює** дані (за це відповідає
  `load_to_duckdb`, але лише якщо ви передаєте їй правильний `ds`) (7 б).

> Перевірка: `airflow dags test github_archive_daily 2024-01-14` двічі поспіль → у
> `raw.github_events_raw` за цей день однакова кількість рядків.

---

## Завдання 5 — backfill і скриншоти · 10 балів

Запустіть backfill за 3 дні і прикладіть скриншоти у `screenshots/`:

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill github_archive_daily -s 2024-01-13 -e 2024-01-15
```

1. **Grid view** DAG-а в UI (http://localhost:8080) із трьома зеленими прогонами (5 б);
2. результат SQL у DuckDB — по одному рядку на кожен із 3 днів (3 б):
   ```sql
   SELECT event_date, count(*) FROM raw.github_events_raw GROUP BY 1 ORDER BY 1;
   ```
3. лог/скрин успішного `airflow dags test ... 2024-01-14` (2 б).

---

## Checkpoint (для самоперевірки)

Після `airflow dags test github_archive_daily 2024-01-14`:

- `raw.github_events_raw` за `2024-01-14`: **172 340** рядків, **5** типів подій;
- розбивка: PushEvent 146 602 · WatchEvent 9 552 · PullRequestEvent 8 601 ·
  IssueCommentEvent 4 699 · IssuesEvent 2 886.

**Definition of done:** `./verify.sh` (з кореня `homework/`) зелений — стек піднявся, DAG
без import errors, має всі задачі, проходить `dags test` і ідемпотентний.
