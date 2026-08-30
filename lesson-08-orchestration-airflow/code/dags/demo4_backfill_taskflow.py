"""Demo 4 (TaskFlow) — ідемпотентність і backfill у декораторному стилі.

Порівняйте з `demo4_backfill.py`:
  demo4_backfill.py          — PythonOperator, `ds` приходить аргументом функції
  demo4_backfill_taskflow.py — @task, `ds` дістаємо з get_current_context()

Ідемпотентність від стилю не залежить: дата береться з logical date, а не з
datetime.now(). Backfill запускається тією самою командою:
  airflow dags backfill demo4_backfill_taskflow -s 2024-03-01 -e 2024-03-03
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from taxi import aggregate, generate_trips


@dag(
    dag_id="demo4_backfill_taskflow",
    schedule="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,                     # історію доганяємо свідомо, через backfill
    tags=["demo", "taskflow"],
)
def demo4_backfill_taskflow():
    @task
    def load_for_day() -> int:
        ds = get_current_context()["ds"]      # дата, за яку обробляємо дані
        trips = generate_trips(ds)
        stats = aggregate(trips)
        print(f"{ds}: завантажено {stats['trips']} поїздок, виручка ${stats['revenue']}")
        return stats["trips"]

    load_for_day()


demo4_backfill_taskflow()
