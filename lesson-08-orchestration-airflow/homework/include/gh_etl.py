"""ETL-хелпери для GitHub Archive — ДАНО. Не редагуйте цей файл.

Це готові «цеглинки», які ваш DAG викликає в задачах. Уся логіка завантаження,
валідації та запису в DuckDB вже реалізована — ваша робота лише оркеструвати ці
функції в Airflow (порядок задач, розклад, ідемпотентність, sensor).

Функції:
    download(ds, landing_dir)      -> шлях до завантаженого файлу (ідемпотентно)
    validate(path)                 -> None; кидає ValueError, якщо файл «битий»
    load_to_duckdb(path, ds, db)   -> int; кількість завантажених подій за день ds
    summarize(ds, db)              -> dict; підсумок за день (rows, event_types)

DuckDB-таблиця, у яку вантажимо:
    raw.github_events_raw(id, event_type, created_at, actor_login, repo_name,
                          public, event_date)
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import urllib.request

import duckdb

log = logging.getLogger("gh_etl")

HOUR = 14  # вантажимо годину 14:00 UTC кожного дня
TARGET_EVENT_TYPES = (
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
)

# вузька схема читання: беремо лише потрібні поля, ігноруючи величезний payload
_READ_COLUMNS = {
    "id": "VARCHAR",
    "type": "VARCHAR",
    "created_at": "TIMESTAMP",
    "actor": "STRUCT(login VARCHAR)",
    "repo": "STRUCT(name VARCHAR)",
    "public": "BOOLEAN",
}

_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.github_events_raw (
    id          VARCHAR,
    event_type  VARCHAR,
    created_at  TIMESTAMP,
    actor_login VARCHAR,
    repo_name   VARCHAR,
    public      BOOLEAN,
    event_date  DATE
);
"""


def archive_url(ds: str) -> str:
    """URL годинного файлу GitHub Archive за дату ds (YYYY-MM-DD)."""
    return f"https://data.gharchive.org/{ds}-{HOUR:02d}.json.gz"


def download(ds: str, landing_dir: str) -> str:
    """Завантажити файл за день ds у landing_dir/<ds>/github_14.json.gz.

    Ідемпотентно: якщо файл уже на місці — повторно не качає.
    """
    dest_dir = os.path.join(landing_dir, ds)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "github_14.json.gz")
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        log.info("Cache hit, reusing %s", dest)
        return dest

    url = archive_url(ds)
    log.info("Downloading %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "gh-etl/1.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    log.info("Downloaded %.1f MB -> %s", os.path.getsize(dest) / 1e6, dest)
    return dest


def validate(path: str) -> None:
    """Перевірити, що файл існує, не порожній і це справді GitHub-події.

    Кидає ValueError, якщо щось не так — у DAG це провалює задачу validate_file.
    """
    if not os.path.exists(path):
        raise ValueError(f"Файл не існує: {path}")
    size_mb = os.path.getsize(path) / 1e6
    if size_mb < 1:
        raise ValueError(f"Файл підозріло малий ({size_mb:.2f} MB): {path}")

    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if i >= 100:
                break
            rec = json.loads(line)
            for field in ("id", "type", "created_at"):
                if field not in rec:
                    raise ValueError(f"У події бракує поля '{field}': {path}")
    log.info("Validation OK (%.1f MB): %s", size_mb, path)


def load_to_duckdb(path: str, ds: str, db_path: str) -> int:
    """Завантажити події за день ds у raw.github_events_raw.

    Ідемпотентно: спершу видаляє рядки за цей event_date, потім вставляє наново.
    Тому повторний запуск того самого дня не дублює дані. Повертає кількість
    завантажених рядків за день ds.
    """
    con = duckdb.connect(db_path)
    try:
        con.execute(_DDL)
        con.execute("DELETE FROM raw.github_events_raw WHERE event_date = ?", [ds])
        types = ", ".join(f"'{t}'" for t in TARGET_EVENT_TYPES)
        cols = "{" + ", ".join(f"'{k}': '{v}'" for k, v in _READ_COLUMNS.items()) + "}"
        con.execute(
            f"""
            INSERT INTO raw.github_events_raw
            SELECT id, type, created_at, actor.login, repo.name, public, DATE '{ds}'
            FROM read_json(?, format='newline_delimited', columns={cols})
            WHERE type IN ({types}) AND repo.name IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY created_at) = 1
            """,
            [path],
        )
        n = con.execute(
            "SELECT count(*) FROM raw.github_events_raw WHERE event_date = ?", [ds]
        ).fetchone()[0]
    finally:
        con.close()
    log.info("Loaded %d events for %s", n, ds)
    return int(n)


def summarize(ds: str, db_path: str) -> dict:
    """Підсумок за день ds: кількість подій і кількість різних типів."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows, types = con.execute(
            "SELECT count(*), count(DISTINCT event_type) "
            "FROM raw.github_events_raw WHERE event_date = ?",
            [ds],
        ).fetchone()
    finally:
        con.close()
    return {"ds": ds, "rows": int(rows), "event_types": int(types)}
