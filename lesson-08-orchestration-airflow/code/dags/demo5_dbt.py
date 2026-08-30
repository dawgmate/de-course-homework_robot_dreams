"""Demo 5 — оркестрація справжнього dbt-проєкту (той самий, що в занятті 05).

download_month → dbt_seed → dbt_bronze → dbt_silver → dbt_gold → dbt_test → report_day

Розподіл ролей:
- **Airflow** — EL і розклад: забрати місячний файл TLC, у правильному порядку смикнути
  шари dbt, поретраїти, показати логи, догнати минулі дні через backfill;
- **dbt** — трансформації: Bronze → Silver → Gold star schema з L05.

Оркестраційна одиниця тут — **шар**, а не окрема модель: `--select tag:bronze`,
`tag:silver`, `tag:gold`. Усередині шару порядок моделей визначає сам dbt за `ref()`.

Ключ до інкрементальності — logical date: `dbt_vars={"ds": "{{ ds }}"}` доїжджає
до моделей як `var('ds')`, і кожна incremental-модель переписує рівно партицію цього дня.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dbt_operator import DbtOperator          # plugins/dbt_operator.py

DB_PATH = "/opt/airflow/data/taxi_dwh.duckdb"
SOURCE_DIR = "/opt/airflow/data/source"
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def download_month(ds, **_):
    """EL-крок: місячний Parquet TLC за місяць `ds`. Ідемпотентно — вже є, не качаємо."""
    import os
    import urllib.request

    month = ds[:7]                                   # 2024-01-15 -> 2024-01
    path = f"{SOURCE_DIR}/yellow_tripdata_{month}.parquet"
    if os.path.exists(path):
        print(f"{path} вже на місці ({os.path.getsize(path) / 1e6:.1f} MB) — пропускаємо")
        return path

    os.makedirs(SOURCE_DIR, exist_ok=True)
    url = TLC_URL.format(month=month)
    print(f"качаємо {url}")
    urllib.request.urlretrieve(url, path + ".part")   # спершу .part, потім атомарний rename
    os.replace(path + ".part", path)
    print(f"готово: {path} ({os.path.getsize(path) / 1e6:.1f} MB)")
    return path


def report_day(ds, **_):
    """Що вийшло за цей день і скільки партицій уже у сховищі."""
    import duckdb

    with duckdb.connect(DB_PATH, read_only=True) as con:
        partitions = con.execute(
            """
            SELECT pickup_date,
                   count(*)                 AS trips,
                   round(sum(total_amount)) AS revenue,
                   max(_loaded_at)          AS loaded_at
            FROM main.gld_fact_trip
            GROUP BY pickup_date
            ORDER BY pickup_date
            """
        ).fetchall()
        top_zones = con.execute(
            """
            SELECT pickup_zone, count(*) AS trips
            FROM main.gld_trip_enriched
            WHERE pickup_date = ?
            GROUP BY pickup_zone
            ORDER BY trips DESC
            LIMIT 5
            """,
            [ds],
        ).fetchall()

    print(f"{'партиція':<12}{'поїздок':>10}{'виручка':>12}   оновлено")
    for pickup_date, trips, revenue, loaded_at in partitions:
        print(f"{pickup_date!s:<12}{trips:>10}{revenue:>12}   {loaded_at:%Y-%m-%d %H:%M:%S}")
    print(f"усього партицій у gld_fact_trip: {len(partitions)}")

    print(f"\nтоп-5 зон посадки за {ds}:")
    for zone, trips in top_zones:
        print(f"  {zone:<28}{trips:>8}")


with DAG(
    dag_id="demo5_dbt",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # DuckDB — один файл і РІВНО один письменник: паралельні runs побились би за lock.
    # Тому backfill іде послідовно. У Postgres/Snowflake це обмеження знімається.
    max_active_runs=1,
    tags=["demo", "dbt"],
) as dag:
    t_download = PythonOperator(task_id="download_month", python_callable=download_month)

    # Довідники (seed_vendor, seed_taxi_zone, …). У проді їх зазвичай ганяють на деплої,
    # а не щодня; тут лишили окремою задачею, щоб було видно всі команди dbt.
    t_seed = DbtOperator(task_id="dbt_seed", command="seed")

    # Шар = задача. {{ ds }} рендериться Airflow-ом, бо dbt_vars — у template_fields.
    t_bronze = DbtOperator(
        task_id="dbt_bronze", command="run", select="tag:bronze", dbt_vars={"ds": "{{ ds }}"}
    )
    t_silver = DbtOperator(
        task_id="dbt_silver", command="run", select="tag:silver", dbt_vars={"ds": "{{ ds }}"}
    )
    t_gold = DbtOperator(
        task_id="dbt_gold", command="run", select="tag:gold", dbt_vars={"ds": "{{ ds }}"}
    )

    t_test = DbtOperator(task_id="dbt_test", command="test")

    t_report = PythonOperator(task_id="report_day", python_callable=report_day)

    t_download >> t_seed >> t_bronze >> t_silver >> t_gold >> t_test >> t_report
