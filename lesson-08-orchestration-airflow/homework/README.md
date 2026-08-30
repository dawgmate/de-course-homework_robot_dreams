# Домашнє завдання — Заняття 08: Оркестрація (Apache Airflow)

## Що робимо

Пишемо **Airflow DAG**, що щодня тягне годину подій **GitHub Archive**, перевіряє їх і
вантажить у локальний DuckDB — повний orchestrated pipeline. ETL-логіка **дана**
(`include/gh_etl.py`) — ви пишете лише **оркестрацію**: сам DAG і custom sensor. Це і є
суть заняття: задачі, залежності, розклад, XCom, sensors та ідемпотентність через logical
date.

- **Специфікація і бали:** [`SPEC.md`](SPEC.md) — головний документ, читайте його.
- **Ваш код:** [`dags/github_archive_daily.py`](dags/github_archive_daily.py)
  і [`plugins/gh_sensor.py`](plugins/gh_sensor.py)
- **Еталон (самоперевірка):** [`../solution/`](../solution/)
- **ETL-цеглинки (дано, не чіпати):** [`include/gh_etl.py`](include/gh_etl.py)

## Передумови

Docker з Compose (стек Airflow піднімається ним). Перевірте: `docker compose version`.

## Як запустити

Усі команди — з кореня `homework/`:

```bash
# 1. .env з прикладу (docker compose читає його автоматично)
cp .env.example .env

# 2. Підняти стек Airflow (перший раз — збере образ, ~хвилина)
docker compose up -d --build

# 3. Відкрити UI: http://localhost:8080  (логін airflow / пароль airflow)
#    Ваш DAG — github_archive_daily.

# 4. Прогнати DAG за один день без планувальника (швидка перевірка):
docker compose exec airflow-scheduler \
  airflow dags test github_archive_daily 2024-01-14

# 5. Подивитися дані у DuckDB:
docker compose exec airflow-scheduler python -c "import duckdb; \
print(duckdb.connect('/opt/airflow/data/github_analytics.duckdb', read_only=True)\
.execute('SELECT event_date, count(*) FROM raw.github_events_raw GROUP BY 1 ORDER BY 1').fetchall())"

# 6. Зупинити:  docker compose down        (з даними у ./data)
#    Повністю:  docker compose down -v
```

Ця директорія (`homework/`) сама монтує ваш код. Щоб подивитись еталон — підніміть стек
із `../solution/` (там та сама структура, `docker compose up -d`).

## Як підходити

1. **Спочатку sensor** (`plugins/gh_sensor.py`) — він найменший. Реалізуйте `poke` з HTTP
   HEAD. Потім — DAG.
2. **DAG збирайте поступово.** Додали задачу — перевірте, що DAG досі парситься:
   ```bash
   docker compose exec airflow-scheduler airflow dags list-import-errors
   ```
   Порожньо = немає помилок імпорту. Список задач:
   ```bash
   docker compose exec airflow-scheduler airflow tasks list github_archive_daily
   ```
3. **Тестуйте через `airflow dags test`** — він проганяє весь DAG за вказаний день
   синхронно, без планувальника. Найшвидший цикл «змінив → перевірив».
4. **Ідемпотентність:** скрізь беріть дату з `{{ ds }}` / `context["ds"]`, ніколи
   `datetime.now()`. Тоді повторний прогін того самого дня не дублює дані.
5. **Стиль на ваш вибір.** Класичний (`PythonOperator` + `xcom_push`/`xcom_pull`) або
   TaskFlow API (`@dag`/`@task`) — оцінюються задачі, залежності й ідемпотентність,
   а не синтаксис. У демо-стеку заняття кожен DAG є в обох стилях — подивіться пару
   `demo2_etl.py` / `demo2_etl_taskflow.py`.

## Самоперевірка

З кореня `homework/` запустіть наскрізну перевірку — вона підніме стек, перевірить, що DAG
парситься й має всі задачі, прожене `airflow dags test` і переконається в
**ідемпотентності** (двічі той самий день → та сама кількість рядків):

```bash
./verify.sh
```

Зелений `PASS ✅` = DAG робочий.

## Що здавати

Pull request зі вмістом цієї директорії (`homework/`):

- `dags/github_archive_daily.py` і `plugins/gh_sensor.py` (ваш код);
- **скриншоти** у `screenshots/` (див. `SPEC.md` → Завдання 5): Grid view з
  backfill за 3 дні, результат SQL по днях, лог успішного `dags test`.

Файли `include/`, `Dockerfile`, `docker-compose.yml` **не змінюйте**.

## Оцінювання — 100 балів

| # | Що | Балів |
|---|---|---|
| 1 | Визначення DAG (id, розклад 06:00 UTC, catchup, start_date, tags) | 20 |
| 2 | Задачі і граф (5 task_id, PythonOperator, залежності, XCom) | 30 |
| 3 | `GHArchiveSensor` (poke з HTTP HEAD, доданий першим, reschedule) | 25 |
| 4 | Ідемпотентність через logical date `{{ ds }}` | 15 |
| 5 | Backfill за 3 дні + скриншоти | 10 |
| | **Разом** | **100** |
