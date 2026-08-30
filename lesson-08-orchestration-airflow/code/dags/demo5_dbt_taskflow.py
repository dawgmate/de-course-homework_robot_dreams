"""Demo 5 (TaskFlow) — той самий dbt-пайплайн, змішаний стиль.

Порівняйте з `demo5_dbt.py`. Тут показано те, як це виглядає у реальних проєктах:
Python-кроки пишуть декораторами (`@task`), а готові/власні оператори лишаються
класичними об'єктами — `DbtOperator` нікуди не дівається. Зв'язати одне з одним
можна прямо через `>>`: XComArg від @task-функції і звичайна задача сумісні.

  download_month → dbt_seed → dbt_bronze → dbt_silver → dbt_gold → dbt_test → report_day

Шлях до файлу їде з `download_month` у `report_day` через XCom — без жодного
xcom_push/xcom_pull, просто аргументом функції.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from dbt_operator import DbtOperator

DB_PATH = "/opt/airflow/data/taxi_dwh.duckdb"
SOURCE_DIR = "/opt/airflow/data/source"
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


@dag(
    dag_id="demo5_dbt_taskflow",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                 # DuckDB — один письменник
    tags=["demo", "dbt", "taskflow"],
)
def demo5_dbt_taskflow():
    @task
    def download_month() -> str:
        """EL-крок: місячний Parquet TLC. Ідемпотентно — вже є, не качаємо."""
        import os
        import urllib.request

        ds = get_current_context()["ds"]
        month = ds[:7]                                   # 2024-01-15 -> 2024-01
        path = f"{SOURCE_DIR}/yellow_tripdata_{month}.parquet"
        if os.path.exists(path):
            print(f"{path} вже на місці ({os.path.getsize(path) / 1e6:.1f} MB) — пропускаємо")
            return path

        os.makedirs(SOURCE_DIR, exist_ok=True)
        url = TLC_URL.format(month=month)
        print(f"качаємо {url}")
        urllib.request.urlretrieve(url, path + ".part")
        os.replace(path + ".part", path)
        print(f"готово: {path} ({os.path.getsize(path) / 1e6:.1f} MB)")
        return path

    @task
    def report_day(source_path: str) -> None:
        """Аргумент `source_path` — це XCom із download_month, витягнутий автоматично."""
        import duckdb

        ds = get_current_context()["ds"]
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

        print(f"джерело: {source_path}")
        print(f"{'партиція':<12}{'поїздок':>10}{'виручка':>12}   оновлено")
        for pickup_date, trips, revenue, loaded_at in partitions:
            print(f"{pickup_date!s:<12}{trips:>10}{revenue:>12}   {loaded_at:%Y-%m-%d %H:%M:%S}")
        print(f"усього партицій у gld_fact_trip: {len(partitions)} (logical date цього run-у: {ds})")

    # Класичні оператори лишаються класичними — TaskFlow їх не витісняє
    seed = DbtOperator(task_id="dbt_seed", command="seed")
    bronze = DbtOperator(
        task_id="dbt_bronze", command="run", select="tag:bronze", dbt_vars={"ds": "{{ ds }}"}
    )
    silver = DbtOperator(
        task_id="dbt_silver", command="run", select="tag:silver", dbt_vars={"ds": "{{ ds }}"}
    )
    gold = DbtOperator(
        task_id="dbt_gold", command="run", select="tag:gold", dbt_vars={"ds": "{{ ds }}"}
    )
    test = DbtOperator(task_id="dbt_test", command="test")

    source_path = download_month()
    source_path >> seed >> bronze >> silver >> gold >> test >> report_day(source_path)


demo5_dbt_taskflow()
