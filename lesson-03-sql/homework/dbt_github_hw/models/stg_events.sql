{{ config(materialized='view') }}
-- =====================================================================
-- TASK 1 — stg_events (12 балів). Специфікація: ../../MODELS.md → «stg_events».
-- Прочитати партиційований Parquet і застосувати DQ-фільтри (типи, боти, порожні push).
-- Нижче — лише контракт колонок (заглушка повертає 0 рядків). Замініть тіло запиту.
-- =====================================================================
SELECT
    id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_name,
    payload_commit_count,
    payload_action,
    payload_ref

FROM read_parquet('{{ var("events_path") }}', hive_partitioning = true),
    
WHERE event_type in ('PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent')
and actor_login not like '%[bot]'  -- TODO: read_parquet(... hive_partitioning=true) + DQ-фільтри
and not ( event_type = 'PushEvent' and payload_commit_count = 0 )
