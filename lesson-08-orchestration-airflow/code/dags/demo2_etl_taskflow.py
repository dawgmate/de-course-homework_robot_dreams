"""Demo 2 (TaskFlow) — ETL з XCom і fan-out без жодного xcom_pull.

Порівняйте з `demo2_etl.py`:
  demo2_etl.py          — PythonOperator + ti.xcom_pull(task_ids="extract")
  demo2_etl_taskflow.py — return значення стає XCom, аргумент функції його забирає

Головне: залежності НЕ задаються через `>>`. Airflow виводить їх із графу викликів —
`validate(trips)` означає «validate після extract». Fan-out виникає сам собою, коли
дві задачі приймають один і той самий результат.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from taxi import aggregate, generate_trips   # plugins/taxi.py на sys.path


@dag(
    dag_id="demo2_etl_taskflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "taskflow"],
)
def demo2_etl_taskflow():
    @task
    def extract() -> list[dict]:
        ds = get_current_context()["ds"]
        trips = generate_trips(ds)
        print(f"extract: {len(trips)} поїздок за {ds}")
        return trips                            # -> XCom автоматично

    @task
    def validate(trips: list[dict]) -> dict:    # аргумент -> XCom підтягується сам
        if not trips:
            raise ValueError("порожній батч — зупиняємо пайплайн")
        stats = aggregate(trips)
        print(f"validate: OK, {stats['trips']} поїздок, виручка ${stats['revenue']}")
        return stats

    @task
    def report_zones(stats: dict) -> None:
        print("поїздки за зонами:")
        for zone, n in sorted(stats["by_zone"].items(), key=lambda kv: -kv[1]):
            print(f"  {zone:<12} {n}")

    @task
    def report_revenue(stats: dict) -> None:
        print(f"загальна виручка за день: ${stats['revenue']}")

    stats = validate(extract())
    report_zones(stats)                         # fan-out: обидві report-задачі
    report_revenue(stats)                       # залежать від validate і йдуть паралельно


demo2_etl_taskflow()
