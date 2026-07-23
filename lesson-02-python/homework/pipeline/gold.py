"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    repo_activity = silver.group_by("repo_name").agg([
        pl.count("event_id").cast(pl.Int64).alias("event_count"),
        pl.n_unique("event_type").cast(pl.Int64).alias("unique_event_types")
    ])
    return repo_activity.write_parquet(config.GOLD_REPO_ACTIVITY)
    

def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    activity_per_minute = silver.with_columns(
        pl.col("created_at").dt.truncate("1m").alias("minute")
    ).group_by("minute").agg([
        pl.count("event_id").cast(pl.Int64).alias("event_count")
    ])
    return activity_per_minute.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE)



def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    commits_by_repo = silver.filter(pl.col("event_type") == "PushEvent").group_by("repo_name").agg([
        pl.count("event_id").cast(pl.Int64).alias("push_count"),
        pl.sum("commit_count").cast(pl.Int64).alias("total_commit_count")
    ])
    return commits_by_repo.write_parquet(config.GOLD_PUSH_COMMITS)
    