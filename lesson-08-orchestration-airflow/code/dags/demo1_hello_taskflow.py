"""Demo 1 (TaskFlow) — той самий «hello», але через декоратори.

Порівняйте з `demo1_hello.py`:
  demo1_hello.py          — BashOperator(task_id=..., bash_command=...)
  demo1_hello_taskflow.py — @task.bash: функція ПОВЕРТАЄ команду, task_id = ім'я функції

Граф той самий (`say_hi >> show_date`), але залежність задається викликом функцій,
а не оператором `>>` між об'єктами-задачами.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="demo1_hello_taskflow",
    schedule=None,                     # тільки ручний trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "taskflow"],
)
def demo1_hello_taskflow():
    @task.bash
    def say_hi() -> str:
        return "echo 'привіт з Airflow'"

    @task.bash
    def show_date() -> str:
        # {{ ds }} рендериться Airflow-ом так само, як у BashOperator
        return "echo 'logical date цього запуску = {{ ds }}'"

    say_hi() >> show_date()            # без обміну даними залежність задають явно


demo1_hello_taskflow()
