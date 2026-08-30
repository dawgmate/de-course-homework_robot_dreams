"""GitHub Archive ingestor — download one hour, filter, load into Postgres.

Цей скрипт ВАМ дано. Не редагуйте його — ваша задача лише контейнеризувати його
(Dockerfile) і запустити в стеку з Postgres (docker-compose.yml).

Скрипт працює всередині контейнера `ingestor` і читає всі налаштування зі змінних
середовища, які задає docker-compose:

    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE  -- підключення до Postgres
    ARCHIVE_URL                                     -- яку годину GitHub Archive вантажити
    CACHE_DIR                                        -- куди кешувати завантажений файл

Завантаження ідемпотентне: якщо година вже в кеші — файл переписується.
Запис ідемпотентний: цільова таблиця очищується (TRUNCATE) перед COPY.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import sys
import urllib.request

import psycopg

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("gh_ingest")

TARGET_EVENT_TYPES = {
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
}

ARCHIVE_URL = os.environ.get(
    "ARCHIVE_URL", "https://data.gharchive.org/2024-01-15-14.json.gz"
)
CACHE_DIR = os.environ.get("CACHE_DIR", "/cache")

DDL = """
CREATE TABLE IF NOT EXISTS github_events (
    id           TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    actor_login  TEXT,
    repo_name    TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL
);
"""


def download(url: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, url.rsplit("/", 1)[-1])
    if os.path.exists(dest):
        log.info("Cache hit, reusing %s", dest)
        return dest
    log.info("Downloading %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "gh-ingest/1.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    log.info("Downloaded %.1f MB", os.path.getsize(dest) / 1e6)
    return dest


def parse(path: str):
    seen: set[str] = set()
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for i, line in enumerate(fh, 1):
            rec = json.loads(line)
            if rec["type"] not in TARGET_EVENT_TYPES:
                continue
            event_id = rec["id"]
            if event_id in seen:
                continue
            repo_name = rec["repo"]["name"]
            if not repo_name:
                continue
            seen.add(event_id)
            yield (
                event_id,
                rec["type"],
                rec["actor"]["login"],
                repo_name,
                rec["created_at"],
            )
            if i % 50_000 == 0:
                log.info("Read %d lines, kept %d", i, len(seen))


def main() -> int:
    conn_info = dict(
        host=os.environ.get("PGHOST", "postgres"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
    )

    src = download(ARCHIVE_URL, CACHE_DIR)

    log.info(
        "Connecting to Postgres at %s:%s/%s",
        conn_info["host"],
        conn_info["port"],
        conn_info["dbname"],
    )
    with psycopg.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE github_events")
            copy_sql = (
                "COPY github_events "
                "(id, event_type, actor_login, repo_name, created_at) FROM STDIN"
            )
            loaded = 0
            with cur.copy(copy_sql) as copy:
                for row in parse(src):
                    copy.write_row(row)
                    loaded += 1
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM github_events")
            total = cur.fetchone()[0]

    log.info("Done. Loaded %d rows (table now has %d).", loaded, total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
