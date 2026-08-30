# Заняття 08. Оркестрація процесів обробки даних: Apache Airflow

## Навіщо це заняття

Типовий Data Engineering pipeline виглядає так: сирий Parquet із NYC Taxi → Bronze → Silver → Gold star schema, усе це запаковано в контейнери. Але є одна серйозна проблема: запускається він **вручну**. Хтось відкриває термінал, набирає команди, і якщо щось впало — про це дізнаються лише наступного ранку, коли аналітик напише «чому даних за вчора немає?»

Справжній production pipeline — це **автоматизоване, плановане, моніторингове виконання**, де:
- pipeline запускається за розкладом без участі людини
- якщо задача впала, вона автоматично перезапускається
- якщо Silver впав — Gold не запускається (залежності між задачами)
- можна побачити, які запуски пройшли успішно, які — впали і чому
- якщо треба догнати минулі місяці — це один рядок у терміналі

Це і є **оркестрація**, а Apache Airflow — найпоширеніший оркестратор у Data Engineering.

Після цього заняття ви зможете написати DAG із кількох взаємозалежних задач, моніторити і дебажити запуски через Airflow UI, зрозумієте, що таке ідемпотентний pipeline, і знатимете, де Airflow — правильний вибір, а де краще подивитись на альтернативи.

## Що треба знати заздалегідь

Загальне уявлення про шари ELT-пайплайну (landing → Bronze → Silver → Gold) і про dbt-моделі. Docker Compose — Airflow піднімається поверх Postgres за тим самим патерном. Базовий Python (функції, декоратори, імпорти). Розуміння того, що таке cron-синтаксис (`0 6 * * *` — щоденно о 6 ранку).

---

## Навіщо оркестрація: проблеми без неї

Уявімо реальну ситуацію: щомісяця о 2:00 ночі з'являється новий файл NYC Taxi за минулий місяць на сервері TLC. Треба його забрати, обробити через medallion (Bronze → Silver → Gold) і до ранку мати готові дані для аналітиків.

**Без оркестрації:**
- Хто запускає? Ви? Вручну? О 2:00 ночі?
- Cron на сервері? Немає retry. Впало — ніхто не знає. Немає залежностей (якщо Silver впав, cron однаково запустить Gold).
- Як довести аналітику, що дані за вчора точно є і вірні?
- Треба завантажити дані за минулі 6 місяців, яких не було. Запускати скрипт 6 разів вручну?

**Airflow вирішує всі ці проблеми** через планування, залежності між задачами, retry з налаштованою затримкою, централізований моніторинг і backfill.

---

## Що таке Apache Airflow

**Apache Airflow — платформа для програмного створення, планування й моніторингу workflow (ETL-процесів).** Ключове слово — «програмного»: pipeline — це **Python-код**, а не клікання в GUI чи YAML-конфіги. Це і сила, і обмеження Airflow.

### Коротка історія

- **Жовтень 2014** — Максім Бошемін (Maxime Beauchemin, також автор Apache Superset) розпочав проєкт в Airbnb для управління складними workflow
- **Березень 2016** — прийнятий у програму Apache Incubator
- **Січень 2019** — отримав статус top-level project Apache Software Foundation
- Сьогодні: де-факто стандарт у Data Engineering; хмарні managed-реалізації у всіх major cloud providers

---

## Архітектура: Scheduler, Executor, Metadata DB, Web UI

```
┌──────────────┐  читає DAG-и   ┌──────────────┐
│  Scheduler   │ ──────────────►│  dags/ folder│
│              │                └──────────────┘
└──────┬───────┘
       │ передає TaskInstances
       ▼
┌──────────────┐   стан      ┌──────────────────┐
│  Executor /  │ ───────────►│  Metadata DB     │
│  Workers     │             │  (Postgres)      │
└──────────────┘             └──────────────────┘
       ▲ моніторинг, manual trigger, логи
┌──────────────┐
│   Web UI     │
└──────────────┘
```

### Scheduler

Серце Airflow. Scheduler:
- **Читає DAG-файли** з `dags/` — постійно, раз на `DAG_DIR_LIST_INTERVAL` (дефолт 5 хвилин, у нашому демо-стеку — 10 секунд). Змінили Python-файл — scheduler підхопить оновлений граф без перезапуску.
- **Будує графік виконання задач**: які залежності між задачами, яка черговість
- **Тригерить runs** за розкладом і перевіряє, що умови виконані
- Реагує на колбеки і змінений стан задач

### Executor і Workers

**Executor** — компонент, що **виконує** задачі, передані Scheduler-ом:

| Executor | Де виконує | Сценарій |
|---|---|---|
| **LocalExecutor** | Subprocess у процесі scheduler | Розробка, малі навантаження |
| **CeleryExecutor** | Окремий Celery-worker pool | Production, горизонтальне масштабування |
| **KubernetesExecutor** | Кожна task у своєму Kubernetes Pod | Ізоляція, різні залежності для tasks |
| **EdgeExecutor** (Airflow 3) | Виконання за межами основного кластера | Edge computing |

Після виконання задачі Executor передає стан `TaskInstance`-а у Metadata DB.

### Metadata DB (Postgres)

Зберігає **весь стан системи**: DAG runs, Task Instances і їх статуси, XCom (дані між задачами), connections, variables, logs. Без Metadata DB Airflow нічого не знає про минулі запуски.

### Web UI

Графічний інтерфейс для моніторингу, manual triggers, перегляду логів, дебагу. Airflow UI — не декоративна надбудова, а основний робочий інструмент оператора pipeline.

---

## DAG: Python-файл як pipeline

**DAG (Directed Acyclic Graph)** — граф задач зі спрямованими залежностями і без циклів. Один DAG = один pipeline. Це **звичайний Python-файл** у папці `dags/`: імпортуєте DAG, визначаєте задачі, задаєте залежності між ними.

```python
# demo1_hello.py — найменший DAG: два BashOperator у лінію
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo1_hello",
    schedule=None,                 # тільки ручний trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo"],
) as dag:
    say_hi = BashOperator(task_id="say_hi", bash_command="echo 'привіт з Airflow'")
    show_date = BashOperator(
        task_id="show_date",
        bash_command="echo 'logical date цього запуску = {{ ds }}'",
    )
    say_hi >> show_date    # залежність: show_date виконається після say_hi
```

Кілька важливих деталей:
- `dag_id` — унікальний ідентифікатор DAG у системі
- `schedule=None` — лише ручний запуск; `schedule="0 6 * * *"` — cron (щоденно о 6:00)
- `catchup=False` — не «доганяти» пропущені intervals від `start_date` автоматично
- `{{ ds }}` — **Jinja-шаблон**: Airflow підставить `logical_date` (дату, за яку обробляються дані)
- `say_hi >> show_date` — синтаксичний цукор для `say_hi.set_downstream(show_date)`

Після того як зберегли Python-файл у `dags/`, Scheduler підхоплює його на наступному скані `dags/` (у демо-стеку — ≤10 секунд). **Код = pipeline.**

---

## Operators, Tasks і XCom

### Ієрархія понять

- **Operator** — клас-шаблон задачі. `BashOperator` — шаблон для виконання shell-команди; `PythonOperator` — для Python-функції.
- **Task** — конкретний instance оператора у конкретному DAG. `say_hi = BashOperator(...)` — це Task.
- **Task Instance** — конкретний запуск Task для конкретного DAG Run. Якщо DAG запускався 10 разів, є 10 Task Instances для кожної задачі.
- **DAG Run** — конкретне виконання DAG для конкретного `logical_date`.

### XCom: передача даних між задачами

**XCom (cross-communication)** — механізм передачі **малих** значень між задачами через Metadata DB.

Важливо: XCom — не для великих даних (файлів, датафреймів). Лише для малих значень: шляхи до файлів, лічильники, статуси. Великі дані передають через shared storage (S3, файлова система).

```python
# demo2_etl.py — extract → validate → (report_zones | report_revenue)

def extract(ds, **_):
    trips = generate_trips(ds)    # ds — дата з Jinja-шаблону
    return trips                  # return-значення автоматично потрапляє в XCom

def validate(ti, **_):
    trips = ti.xcom_pull(task_ids="extract")    # забираємо з XCom
    if not trips:
        raise ValueError("порожній батч — зупиняємо пайплайн")
    return aggregate(trips)
```

### Залежності і топологія

```python
# Лінійна: a → b → c
a >> b >> c

# Fan-out: validate → [report_zones, report_revenue] паралельно
t_validate >> [t_zones, t_revenue]

# Fan-in: [a, b, c] → d (d чекає на всі три)
[a, b, c] >> d
```

Fan-out особливо корисний: `t_zones` і `t_revenue` не залежать одна від одної, тож LocalExecutor запустить їх паралельно в двох subprocess-ах. Замість послідовного `30 + 30 = 60 секунд` отримуємо `30 секунд`.

---

## Логічні концепти: Sensors, Hooks, Connections, Variables, Providers

### Sensor: чекати на умову

**Sensor** — особливий Operator, що **чекає** на виконання умови, перш ніж пропустити flow далі. Умова перевіряється через метод `poke()`:

```python
def poke(self, context) -> bool:
    # Перевірити умову. True → умова виконана, рухаємось далі
    # False → ще чекаємо
    return check_condition()
```

Режими роботи:
- `mode="poke"` — процес тримає worker-слот і перевіряє кожні `poke_interval` секунд
- `mode="reschedule"` — між перевірками **звільняє** worker-слот (більш ефективно при довгому очікуванні)

Приклад: `FileSensor` чекає появи файлу; `HttpSensor` — відповіді HTTP-сервісу; `ExternalTaskSensor` — завершення задачі в іншому DAG.

### Hook: обгортка над зовнішньою системою

**Hook** інкапсулює клієнт і логіку підключення до зовнішньої системи. Замість того, щоб кожен Operator самостійно відкривав з'єднання з Postgres (і дублював код) — всі вони використовують `PostgresHook`. Hook читає credentials з Connection за `conn_id` і надає готовий клієнт.

### Connection: збережені credentials

**Connection** — іменований набір credentials/параметрів підключення, збережений у Metadata DB (або secret-менеджері). Operator/Hook посилається на Connection за `conn_id`, не хардкодить credentials у коді.

Приклад: `postgres_default` — Connection для локального Postgres. Хуки читають `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` з нього.

### Variable: конфігурація DAG-ів

**Variable** — пара ключ-значення, доступна з коду DAG і через Web UI. Зручна для шляхів, прапорців, параметрів, які можуть змінюватись без деплою нового коду.

```python
from airflow.models import Variable
landing_dir = Variable.get("landing_dir", default_var="/data/landing")
```

### Provider: пакети розширень

Ядро Airflow — тонке. Інтеграції з конкретними системами живуть у **Provider-пакетах**:
- `apache-airflow-providers-postgres` — PostgresOperator, PostgresHook
- `apache-airflow-providers-amazon` — S3Hook, S3Sensor, RedshiftOperator
- `apache-airflow-providers-http` — HttpOperator, HttpSensor

Встановлюються через `pip install apache-airflow-providers-<name>`.

---

## Моніторинг, retries і дебаг через UI

У production задачі падають — мережа, пам'ять, неочікувані дані. Airflow дає retry з коробки.

```python
# demo3_retry.py — задача, що навмисно падає з першої спроби
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(seconds=10)}

def flaky(ti, **_):
    if ti.try_number == 1:
        raise RuntimeError("навмисний збій — Airflow зробить retry за 10 с")
    print(f"успіх зі спроби #{ti.try_number}")
```

### Airflow UI: три ключові view

**DAGs view** — список усіх DAG-ів: статус останнього run, розклад, toggle паузи. За замовчуванням новий DAG — **на паузі**: активуйте його через toggle.

**Grid view** — матриця task runs × dag runs: кожна клітинка — це Task Instance. Кольори: зелений = success, жовтий = `up_for_retry`, червоний = failed, сірий = skipped.

**Graph view** — граф залежностей DAG-у: видно топологію і статус кожної задачі для конкретного run.

### Дебаг-петля: щоденна робота

1. Побачили `failed` у Grid view → клік на клітинку
2. **View Logs** → читаємо traceback (Airflow копіює всі `print()` і `logging` у logs)
3. Виправили причину (код, дані, конфіг)
4. **Clear** — перезапускає **тільки цю задачу** (і, якщо потрібно, downstream) без перезапуску всього DAG
5. Або **Mark Success** — якщо failure несуттєвий і можна рухатись далі

**Важливо:** не «перезапусти весь DAG», а **точково**: Clear конкретної задачі. Це економить час і не порушує вже успішні задачі.

---

## Ідемпотентність і backfill: надійні pipeline-и

### Logical date vs реальний час

Одна з найважливіших концепцій Airflow — відмінність між **logical date** (дата, за яку обробляються дані) і реальним часом виконання.

**Погано (не ідемпотентно):**
```python
def load_data(**_):
    today = datetime.now().strftime("%Y-%m-%d")  # реальний час!
    trips = fetch_trips(today)
```

Якщо цю задачу запустити двічі в один день — обидва рази вона завантажить дані «за сьогодні». Якщо запустити завтра — вона завантажить «за завтра». Це не можна перевикористати для backfill.

**Добре (ідемпотентно):**
```python
def load_for_day(ds, **_):
    # ds — Airflow передає дату з Jinja-контексту: "2024-03-01"
    trips = generate_trips(ds)
    stats = aggregate(trips)
    print(f"{ds}: завантажено {stats['trips']} поїздок")
```

Задача залежить від `ds` (logical date). Повторний запуск за `ds="2024-03-01"` дасть той самий результат. Це і є ідемпотентність на рівні Airflow.

### catchup=False і backfill

```python
with DAG(dag_id="demo4_backfill", schedule="@daily",
         start_date=datetime(2024, 3, 1), catchup=False, ...):
    PythonOperator(task_id="load_for_day", python_callable=load_for_day)
```

`catchup=False` — при анпаузі DAG Airflow **не** доганяє автоматично всі пропущені intervals від `start_date` до сьогодні. Без цього прапорця Airflow запустить всі дні з 1 березня до сьогодні одразу — лавина runs, що може поклати базу.

Коли треба свідомо догнати минулі дати — використовуємо **backfill**:

```bash
# Три DAG runs: за 2024-03-01, 2024-03-02, 2024-03-03
airflow dags backfill demo4_backfill -s 2024-03-01 -e 2024-03-03
```

Airflow запустить три `load_for_day` паралельно (або послідовно, залежно від налаштувань), кожен зі своїм `ds`.

### Ідемпотентні патерни для задач

Аби повторний запуск не дублював дані:
- `TRUNCATE + INSERT` (або dbt `materialized='table'`)
- `INSERT OR IGNORE` / `ON CONFLICT DO NOTHING` за первинним ключем
- Partition overwrite (Spark/Iceberg): перезаписати партицію за датою

---

## Custom Operators і Sensors

Коли стандартних Operator-ів не вистачає — пишемо власний клас, наслідуючи від `BaseOperator` або `BaseSensorOperator`:

```python
# Sensor, що чекає появи годинного файлу через HTTP HEAD
from airflow.sensors.base import BaseSensorOperator
import urllib.request

class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs):
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "gh-sensor/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200      # 200 → файл є, рухаємось далі
        except Exception:
            return False                       # ще чекаємо
```

`poke()` повертає `True`, коли умова виконана, `False` — продовжувати чекати. Sensor ставлять першою задачею DAG з `timeout` (max час очікування), `poke_interval` і `mode="reschedule"`.

---

## Як оркеструють dbt-проєкти

У більшості компаній трансформації живуть у dbt, а Airflow цим dbt керує. Розподіл ролей
жорсткий: **Airflow — «коли, в якому порядку, що робити при падінні»; dbt — «який саме SQL»**.
dbt не має розкладу, retry, алертів і backfill-у; Airflow не знає нічого про моделі й `ref()`.

### Крок 1. Обрати гранулярність задач

Це головне рішення. Один і той самий dbt-проєкт можна запускати чотирма способами:

| Гранулярність | Як виглядає | Плюси | Мінуси |
|---|---|---|---|
| **1 задача на проєкт** | `BashOperator("dbt build")` | найпростіше, атомарно | впала одна модель — у Grid view один червоний квадрат, ретраїш усе |
| **1 задача на шар/домен** | `dbt run --select tag:bronze`, потім `tag:silver`… | видно, де саме зупинився пайплайн; можна вклинити не-dbt кроки між шарами | шар усе ще перезапускається цілком |
| **1 задача на модель** | `astronomer-cosmos` парсить `manifest.json` і генерує Airflow-задачі | повний лінидж у Graph view, точковий retry, паралелізм | більше задач у метабазі, залежність від структури `manifest.json` |
| **тригер job у dbt Cloud** | `DbtCloudRunJobOperator` | dbt виконує SaaS, Airflow лише диригує | лінидж і логи — в іншій системі |

Робоче правило: маленький проєкт — одна задача; типовий medallion — **задача на шар**
(так зроблено в `demo5_dbt`); великий проєкт із багатьма командами — Cosmos.

### Крок 2. Передати «за який день рахуємо»

Airflow має рівно одну річ, якої немає в dbt, — **logical date**. Її передають як dbt-змінну:

```python
DbtOperator(task_id="dbt_bronze", command="run",
            select="tag:bronze", dbt_vars={"ds": "{{ ds }}"})
# -> dbt run --select tag:bronze --vars '{"ds": "2024-01-15"}'
```

### Крок 3. Підготувати моделі до інкрементальної загрузки

Щоб dbt-проєкт можна було ганяти щодня, у моделі додають чотири речі:

1. **поле-партиція** — колонка, за якою ріжуться дані (`pickup_date`);
2. **audit-колонка** `_loaded_at` — коли партицію переписали востаннє;
3. **матеріалізація** `incremental` з `unique_key` і стратегією `delete+insert`;
4. **фільтр за `var('ds')`** — щоб прогін читав рівно свою добу.

```sql
-- models/bronze/brz_yellow_trips.sql
{{ config(materialized='incremental', unique_key='pickup_date',
          incremental_strategy='delete+insert') }}

select
    ...,
    cast(tpep_pickup_datetime as date) as pickup_date,   -- поле-партиція
    current_timestamp                  as _loaded_at     -- audit
from {{ source('nyc_tlc', 'yellow_trips_raw') }}
{% if var('ds') is not none %}
where cast(tpep_pickup_datetime as date) = date '{{ var("ds") }}'
{% endif %}
```

Що це дає:
- **читається один день**, а не вся історія — саме тут економія часу й грошей;
- **`delete+insert` за `unique_key`** — dbt спершу видаляє рядки цього дня, потім вставляє нові,
  тож повторний прогін **не дублює** дані. Ідемпотентність забезпечує матеріалізація dbt,
  а не ваш Python-код;
- **backfill** за минулі дати — послідовність тих самих прогонів із різними `ds`.

Не все треба робити інкрементальним. Практика така:

| Що | Матеріалізація | Чому |
|---|---|---|
| довідники (`seed_*`), виміри | `seed` / `table` | маленькі, дешевше перебудувати цілком |
| bronze / silver / факти | `incremental` за партицією | ростуть щодня, перебудова з нуля марна |
| агрегати-вітрини | `table` | рахуються по всій історії, але зазвичай маленькі |

### Крок 4. Не забути про операційні деталі

- **dbt в окремому venv або контейнері** — dbt і Airflow пінять несумісні версії спільних
  залежностей (jinja2, click, protobuf). Варіанти: venv в образі, `DockerOperator`,
  `KubernetesPodOperator`, Cosmos із `ExecutionConfig`.
- **`profiles.yml` — у репозиторії, секрети — з оточення** (`env_var`) або з Airflow Connection.
- **`dbt build` vs `run` + `test`** — `build` виконує модель і одразу її тести (швидше валить
  пайплайн на брудних даних); окремі `run` і `test` дають зрозуміліший Grid view.
- **`dbt source freshness`** першою задачею — перевірка, що джерело оновилось (аналог сенсора).
- **`--full-refresh` тільки свідомо**: для партиційованої моделі він перебудує таблицю з даних
  ЛИШЕ поточного `ds`. Після зміни логіки моделі історію повертають backfill-ом.
- **Артефакти `target/manifest.json` і `run_results.json`** — джерело для Cosmos, лінидж-каталогів
  і моніторингу (скільки рядків, скільки тривала кожна модель).

> **Обережно з одночасністю.** Якщо сховище має одного письменника (DuckDB, SQLite), паралельні
> DAG runs поб'ються за lock — ставте `max_active_runs=1`. Для Postgres/Snowflake/BigQuery це
> обмеження не діє.

---

## TaskFlow API: сучасний стиль DAG-ів

TaskFlow API (з Airflow 2.0) дозволяє писати DAG через декоратори `@dag` і `@task` замість явних класів-operators:

```python
from airflow.decorators import dag, task
from airflow.utils.context import get_current_context

@task
def download_archive() -> str:
    ds = get_current_context()["ds"]
    path = download(ds, LANDING_DIR)
    return path                   # return → XCom автоматично

@task
def validate_file(file_path: str) -> str:    # аргумент → XCom витягується автоматично
    validate(file_path)
    return file_path

@task
def load_to_duckdb(file_path: str) -> int:
    return load(file_path, DB_PATH)

@dag(schedule="0 6 * * *", start_date=datetime(2024, 1, 1), catchup=False)
def github_archive_daily():
    path = download_archive()
    validated = validate_file(path)
    rows = load_to_duckdb(validated)
    # залежності виводяться автоматично з графу викликів
```

Ключові відмінності від класичного стилю:

| | Класичний стиль | TaskFlow API |
|---|---|---|
| задача | `PythonOperator(task_id=..., python_callable=f)` | `@task def f()` — `task_id` = ім'я функції |
| shell-команда | `BashOperator(bash_command="…")` | `@task.bash def f(): return "…"` |
| дані між задачами | `return` → `ti.xcom_pull(task_ids="…")` | `return` → аргумент наступної функції |
| залежності | явно: `a >> b` | з графу викликів: `b(a())` |
| контекст (`ds`, `ti`) | аргумент callable: `def f(ds, ti, **_)` | `get_current_context()["ds"]` |
| ретраї | `default_args={"retries": 2}` | `@task(retries=2)` на конкретній задачі |

Обидва підходи — правильні, і **вони змішуються**: у TaskFlow-DAG-у спокійно живуть звичайні
оператори (сенсори, `DbtOperator`), приєднані через `>>`. TaskFlow лаконічніший там, де багато
Python і передачі даних; класичний стиль — там, де DAG складається переважно з готових операторів.

У демо-стеку кожен `demoN_*.py` має двійника `demoN_*_taskflow.py` — той самий пайплайн у
другому стилі. Відкрийте пару файлів поруч і порівняйте.

---

## Best Practices

- **Ідемпотентність** — кожна задача залежить від `ds`, а не від `datetime.now()`. Повторний run = той самий результат.
- **Не імпортуйте важкі бібліотеки на топ-рівні DAG-файлу** — Scheduler парсить `dags/` часто. `import pandas`, `import torch` всередині callable, не на рівні модуля.
- **Використовуйте time-zone-aware дати** (`pendulum.datetime(2024, 1, 1, tz="UTC")`).
- **`catchup=False`** для нових DAG-ів, backfill — свідомо через CLI.
- **`.airflowignore`** — виключити з парсингу файли в `dags/`, що не є DAG-ами.
- **Connections і Variables** — для credentials і конфігу, не хардкод у коді.
- **Обирайте Executor під задачу:** LocalExecutor — для dev; Celery або Kubernetes — для production.
- **Розгляньте managed Airflow** (Astronomer, MWAA, Cloud Composer) замість самостійного хостингу: ops overhead значний.

---

## Managed Airflow і альтернативи

### SaaS-рішення (zero-ops Airflow)

| Сервіс | Провайдер | Особливості |
|---|---|---|
| **Astronomer** | Astronomer Inc. | Airflow-спеціалізований SaaS, enterprise support |
| **Google Cloud Composer** | GCP | Managed Airflow на Google Kubernetes Engine |
| **Amazon MWAA** | AWS | Managed Workflows for Apache Airflow |

### Альтернативні оркестратори

| Інструмент | Модель | Ніша |
|---|---|---|
| **Prefect** | Python-first, dynamic DAGs | Стартапи, команди без DevOps |
| **Dagster** | Asset-based (не task-based) | Lakehouse-стеки, data lineage з коробки |
| **Luigi** | Python, task-based | Прості batch pipeline-и |
| **Apache NiFi** | no-code, drag-and-drop UI | Data flow, enterprise integration |
| **Palantir Foundry** | Ontology-based visual modeling | Enterprise, ontology-driven |

### Asset-based модель (Dagster): принциповий зсув

Airflow думає «tasks»: виконай цей крок, потім той. **Dagster** думає «assets»: матеріалізуй цю таблицю, цей файл. Ви оголошуєте, що є результатом, а не як до нього дійти крок за кроком.

Це ближче до того, як Data Engineer думає про свою роботу: «Gold-таблиця залежить від Silver, Silver від Bronze». Airflow рухається у тому самому напрямку — концепт **Assets** вже з'явився у Executor Airflow 3.

---

## Ключові терміни

| Термін | Визначення |
|---|---|
| **DAG (Directed Acyclic Graph)** | Граф задач зі спрямованими залежностями і без циклів. Один пайплайн; звичайний Python-файл у `dags/`. |
| **Operator** | Клас-шаблон задачі: BashOperator, PythonOperator, або custom. |
| **Task** | Instance оператора у конкретному DAG. |
| **Task Instance** | Конкретний запуск Task для конкретного DAG Run (одна «клітинка» в Grid view). |
| **DAG Run** | Конкретне виконання DAG для конкретного `logical_date`. |
| **Sensor** | Operator, що чекає на умову; `poke()` повертає True/False; `mode="reschedule"` звільняє слот між спробами. |
| **Hook** | Обгортка над зовнішньою системою (Postgres, S3, HTTP): інкапсулює підключення. |
| **Connection** | Збережені credentials/параметри підключення (в Metadata DB або secret-менеджері); ідентифікується за `conn_id`. |
| **Variable** | Конфіг-пара ключ-значення для DAG-ів; доступна з коду і Web UI. |
| **Provider** | Пакет-розширення з operators/hooks/sensors під конкретну систему. |
| **XCom** | Cross-communication: передача малих значень між задачами через Metadata DB. |
| **Scheduler** | Читає `dags/`, будує графік, тригерить runs, перевіряє залежності. |
| **Executor** | Виконує задачі, передані Scheduler-ом. Типи: Local, Celery, Kubernetes, Edge. |
| **Metadata DB** | Postgres: зберігає весь стан Airflow (runs, task instances, XCom, connections, variables). |
| **logical_date** | Дата, за яку обробляються дані в DAG Run. Не час виконання (`datetime.now()`). |
| **catchup** | Прапорець: чи доганяти автоматично пропущені intervals від `start_date`. |
| **backfill** | Свідомий запуск DAG за минулі дати через CLI (`airflow dags backfill`). |
| **Retry** | Автоматичний повтор Task після падіння, через `retry_delay`. |
| **Ідемпотентність** | Задача залежить від `ds`, не від `datetime.now()`: повторний run = той самий результат. |
| **TaskFlow API** | Декоратори `@dag`/`@task`: XCom і залежності виводяться автоматично з Python-коду. |
| **Asset** | Логічний об'єкт-результат виконання задачі (таблиця, файл). |
| **Incremental-модель (dbt)** | Матеріалізація, що на кожному прогоні дописує/переписує лише нову партицію, а не будує таблицю з нуля. |
| **`max_active_runs`** | Скільки DAG runs одного DAG-у можуть виконуватись одночасно (`1` — для сховищ з одним письменником). |

---

## Перевір себе

1. Назвіть чотири конкретні проблеми, які виникають, якщо pipeline запускається через cron без оркестратора. Як Airflow вирішує кожну з них?
2. Що таке Scheduler в Airflow? Що станеться, якщо ви збережете змінений DAG-файл — коли Scheduler це підхопить і через скільки?
3. Поясніть різницю між Operator, Task і Task Instance. Що таке DAG Run? Скільки Task Instances може бути для однієї Task, якщо DAG запускався 30 разів?
4. Навіщо в Airflow є XCom і чому не можна передавати через нього великі дані (датафрейми, файли)?
5. Чому задача повинна залежати від `ds` (logical date), а не від `datetime.now()`? Наведіть конкретний сценарій, де не-ідемпотентна задача спричинить проблему.
6. Що таке `catchup=False` і навіщо він потрібен? Що трапиться, якщо забути про нього при анпаузі DAG з `start_date` 6 місяців тому?
7. У чому різниця між класичним стилем DAG (PythonOperator + xcom_push/pull) і TaskFlow API (@task)? Коли кожен підходить?
8. Що робить incremental-модель dbt на першому прогоні і що — на всіх наступних? Навіщо їй передавати `ds` з Airflow і що станеться, якщо цього не зробити?
9. Порівняйте Airflow і Dagster за моделлю мислення (task-based vs asset-based). Для якого сценарію Dagster може бути кращим вибором?
