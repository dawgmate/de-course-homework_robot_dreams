"""Demo 3 (TaskFlow) — retries на рівні декоратора.

Порівняйте з `demo3_retry.py`:
  demo3_retry.py          — retries/retry_delay у default_args цілого DAG-у
  demo3_retry_taskflow.py — @task(retries=2, retry_delay=...) на КОНКРЕТНІЙ задачі

Поведінка в UI однакова: жовтий `up_for_retry`, потім зелений; Clear і View Logs
працюють так само — TaskFlow міняє лише спосіб опису, а не механіку Airflow.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    dag_id="demo3_retry_taskflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "taskflow"],
)
def demo3_retry_taskflow():
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def flaky() -> int:
        ti = get_current_context()["ti"]
        # try_number == 1 -> це перша спроба; падаємо, щоб спрацював retry
        if ti.try_number == 1:
            raise RuntimeError("навмисний збій — Airflow зробить retry за 10 c")
        print(f"успіх зі спроби #{ti.try_number}")
        return ti.try_number

    @task
    def downstream(attempt: int) -> None:
        print(f"downstream пішов лише після того, як flaky став зеленим зі спроби #{attempt}")

    downstream(flaky())


demo3_retry_taskflow()
