# Образ демо-стека: стоковий Airflow + duckdb + dbt.
# demo1–demo4 — чистий stdlib і працюють і на голому apache/airflow:2.10.5;
# цей тонкий шар потрібен лише для demo5 (dbt поверх DuckDB).

FROM apache/airflow:2.10.5

# duckdb для Airflow-задач demo5: запис сирої партиції і читання вітрини.
# Версія та сама, що всередині dbt-venv, — файл БД пишуть обидва процеси.
RUN pip install --no-cache-dir duckdb==1.5.4

# dbt тримаємо в ОКРЕМОМУ venv: dbt і Airflow пінять несумісні версії спільних
# залежностей (jinja2, click, protobuf), тож один site-packages вони не ділять.
# Це стандартний прод-патерн; DbtOperator викликає саме цей бінарник.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.11.12" \
        "dbt-duckdb==1.10.1" \
        "duckdb==1.5.4"
