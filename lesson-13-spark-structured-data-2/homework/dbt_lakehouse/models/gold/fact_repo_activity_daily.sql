
with commits_daily as (
    select
        repo_name,
        to_date(pushed_at) as activity_day,
        count(*) as commits,
        count(distinct author_email) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('commits') }}
    group by
        repo_name,
        to_date(pushed_at)
),

prs_opened_daily as (
    select
        repo_name,
        to_date(opened_at) as activity_day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        count(*) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('pull_requests') }}
    where opened_at is not null
    group by
        repo_name,
        to_date(opened_at)
),

prs_merged_daily as (
    select
        repo_name,
        to_date(merged_at) as activity_day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        count(*) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by
        repo_name,
        to_date(merged_at)
),

issues_opened_daily as (
    select
        repo_name,
        to_date(opened_at) as activity_day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        count(*) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('issues') }}
    where opened_at is not null
    group by
        repo_name,
        to_date(opened_at)
),

issues_closed_daily as (
    select
        repo_name,
        to_date(closed_at) as activity_day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        count(*) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('issues') }}
    where closed_at is not null
    group by
        repo_name,
        to_date(closed_at)
),

repo_events_daily as (
    select
        repo_name,
        to_date(created_at) as activity_day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        sum(
            case
                when event_type = 'WatchEvent' then 1
                else 0
            end
        ) as stars,
        sum(
            case
                when event_type = 'ForkEvent' then 1
                else 0
            end
        ) as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
    group by
        repo_name,
        to_date(created_at)
),

all_activity as (
    select * from commits_daily
    union all
    select * from prs_opened_daily
    union all
    select * from prs_merged_daily
    union all
    select * from issues_opened_daily
    union all
    select * from issues_closed_daily
    union all
    select * from repo_events_daily
),

daily_rollup as (
    select
        repo_name,
        activity_day,
        sum(commits) as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened) as prs_opened,
        sum(prs_merged) as prs_merged,
        sum(issues_opened) as issues_opened,
        sum(issues_closed) as issues_closed,
        sum(stars) as stars,
        sum(forks) as forks
    from all_activity
    group by
        repo_name,
        activity_day
),

with_keys as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(activity_day, 'yyyyMMdd') as int) as date_id,
        commits,
        distinct_committers,
        prs_opened,
        prs_merged,
        issues_opened,
        issues_closed,
        stars,
        forks
    from daily_rollup
)

select
    md5(
        concat_ws(
            '|',
            repo_id,
            cast(date_id as string)
        )
    ) as activity_id,
    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from with_keys
