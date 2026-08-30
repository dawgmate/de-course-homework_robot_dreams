"""Demo 2 — справжній ETL на PythonOperator з XCom і fan-out.

extract → validate → (report_zones | report_revenue)

- PythonOperator виконує звичайну Python-функцію.
- Дані між задачами їдуть через XCom (return значення -> xcom_pull).
- Дві report-задачі залежать від validate і йдуть ПАРАЛЕЛЬНО (fan-out).
Усі результати друкуються в task logs — їх читаємо в UI.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from taxi import generate_trips, aggregate   # plugins/taxi.py на sys.path


def extract(ds, **_):
    trips = generate_trips(ds)
    print(f"extract: {len(trips)} поїздок за {ds}")
    return trips                                # -> XCom


def validate(ti, **_):
    trips = ti.xcom_pull(task_ids="extract")    # читаємо XCom попередньої задачі
    if not trips:
        raise ValueError("порожній батч — зупиняємо пайплайн")
    stats = aggregate(trips)
    print(f"validate: OK, {stats['trips']} поїздок, виручка ${stats['revenue']}")
    return stats


def report_zones(ti, **_):
    stats = ti.xcom_pull(task_ids="validate")
    print("поїздки за зонами:")
    for zone, n in sorted(stats["by_zone"].items(), key=lambda kv: -kv[1]):
        print(f"  {zone:<12} {n}")


def report_revenue(ti, **_):
    stats = ti.xcom_pull(task_ids="validate")
    print(f"загальна виручка за день: ${stats['revenue']}")


with DAG(
    dag_id="demo2_etl",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo"],
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_validate = PythonOperator(task_id="validate", python_callable=validate)
    t_zones = PythonOperator(task_id="report_zones", python_callable=report_zones)
    t_revenue = PythonOperator(task_id="report_revenue", python_callable=report_revenue)

    t_extract >> t_validate >> [t_zones, t_revenue]    # fan-out на дві паралельні задачі
