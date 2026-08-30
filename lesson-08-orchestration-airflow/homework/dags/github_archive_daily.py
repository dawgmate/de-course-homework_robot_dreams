"""github_archive_daily — ВАШ DAG. Специфікація: ../SPEC.md → «DAG».

Готові ETL-цеглинки вже є — імпортуйте і викликайте їх у задачах (не переписуйте):

    from include.gh_etl import download, validate, load_to_duckdb, summarize
    from gh_sensor import GHArchiveSensor   # ваш custom sensor із plugins/

Що треба зібрати (деталі й бали — у SPEC.md):
  * DAG `github_archive_daily`, розклад «щодня о 06:00 UTC», catchup=False;
  * усі задачі працюють із logical date {{ ds }}, а не datetime.now() — це дає
    ідемпотентність і коректний backfill;
  * граф:
        check_availability -> download_archive -> validate_file
            -> load_to_duckdb -> notify_completion
  * download_archive кладе шлях у XCom; validate_file і load_to_duckdb беруть його з XCom;
  * шляхи (дано):
        DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
        LANDING_DIR = "/opt/airflow/data/landing"

Перевірка: `airflow dags test github_archive_daily 2024-01-14` має пройти всі задачі;
наскрізно — `./verify.sh` із кореня homework/.
"""

from __future__ import annotations

# TODO: реалізуйте DAG згідно зі SPEC.md.
raise NotImplementedError("Реалізуйте DAG github_archive_daily — див. SPEC.md")
