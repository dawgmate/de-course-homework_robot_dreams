"""Demo 4 — ідемпотентність через logical_date і backfill.

Задача бере дату НЕ з datetime.now(), а з `ds` (logical date цього run-у). Тому:
- повторний прогін того самого дня дає ту саму кількість рядків (ідемпотентно);
- backfill за минулі дні дає по одному правильному run-у на кожну дату.

Показуємо `airflow dags backfill demo4_backfill -s 2024-03-01 -e 2024-03-03`
-> три DAG runs, кожен зі своїм `ds`, у Grid view.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from taxi import generate_trips, aggregate


def load_for_day(ds, **_):
    # ds — дата, за яку обробляємо дані; ніколи не datetime.now()
    trips = generate_trips(ds)
    stats = aggregate(trips)
    print(f"{ds}: завантажено {stats['trips']} поїздок, виручка ${stats['revenue']}")
    return stats["trips"]


with DAG(
    dag_id="demo4_backfill",
    schedule="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,                     # не доганяємо історію автоматично — робимо це руками через backfill
    tags=["demo"],
) as dag:
    PythonOperator(task_id="load_for_day", python_callable=load_for_day)
