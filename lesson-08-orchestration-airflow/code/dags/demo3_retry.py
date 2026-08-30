"""Demo 3 — retries і дебаг через UI.

`flaky` навмисно падає з ПЕРШОЇ спроби і проходить з ДРУГОЇ. На парі показуємо:
- retries=2 + retry_delay: Airflow сам перезапускає задачу;
- у Grid view видно жовтий "up_for_retry", потім зелений;
- View Logs -> traceback; Clear -> ручний перезапуск окремої задачі.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}


def flaky(ti, **_):
    # try_number == 1 -> це перша спроба; падаємо, щоб спрацював retry
    if ti.try_number == 1:
        raise RuntimeError("навмисний збій — Airflow зробить retry за 10 c")
    print(f"успіх зі спроби #{ti.try_number}")


def downstream(**_):
    print("downstream пішов лише після того, як flaky нарешті став зеленим")


with DAG(
    dag_id="demo3_retry",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["demo"],
) as dag:
    t_flaky = PythonOperator(task_id="flaky", python_callable=flaky)
    t_down = PythonOperator(task_id="downstream", python_callable=downstream)

    t_flaky >> t_down
