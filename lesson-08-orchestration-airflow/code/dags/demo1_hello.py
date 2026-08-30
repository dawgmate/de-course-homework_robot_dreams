"""Demo 1 — найменший можливий DAG: два BashOperator у лінію.

Мета на парі: показати, що DAG — це просто Python-файл, який повертає граф задач.
Запускаємо вручну (schedule=None) і дивимось у Grid view, як зеленіють дві задачі.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo1_hello",
    schedule=None,                     # тільки ручний trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo"],
) as dag:
    say_hi = BashOperator(
        task_id="say_hi",
        bash_command="echo 'привіт з Airflow'",
    )

    # {{ ds }} — Jinja-шаблон: Airflow підставить logical date цього run-у
    show_date = BashOperator(
        task_id="show_date",
        bash_command="echo 'logical date цього запуску = {{ ds }}'",
    )

    say_hi >> show_date                # залежність: show_date після say_hi
