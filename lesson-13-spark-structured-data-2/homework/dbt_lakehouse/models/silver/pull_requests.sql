
with parsed_events as (
    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("pr_schema") }}') as parsed_payload
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'
),

prepared as (
    select
        event_id,
        repo_name,
        parsed_payload.number as pr_number,
        parsed_payload.pull_request.title as title,
        parsed_payload.pull_request.user.login as author_login,
        parsed_payload.pull_request.state as state,
        parsed_payload.pull_request.merged as is_merged,
        parsed_payload.pull_request.draft as is_draft,
        to_timestamp(
            parsed_payload.pull_request.created_at
        ) as opened_at,
        to_timestamp(
            parsed_payload.pull_request.closed_at
        ) as closed_at,
        to_timestamp(
            parsed_payload.pull_request.merged_at
        ) as merged_at,
        parsed_payload.pull_request.additions as additions,
        parsed_payload.pull_request.deletions as deletions,
        parsed_payload.pull_request.changed_files as changed_files,
        parsed_payload.pull_request.commits as commits_count,
        parsed_payload.pull_request.comments as comments,
        parsed_payload.pull_request.review_comments as review_comments,
        parsed_payload.pull_request.author_association as author_association,
        transform(
            parsed_payload.pull_request.labels,
            label -> label.name
        ) as label_names,
        parsed_payload.action as last_action,
        event_at as last_event_at
    from parsed_events
),

ranked as (
    select
        *,
        row_number() over (
            partition by repo_name, pr_number
            order by last_event_at desc, event_id desc
        ) as row_num
    from prepared
),

latest as (
    select *
    from ranked
    where row_num = 1
)

select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,
    additions + deletions as churn,
    (
        unix_timestamp(coalesce(closed_at, last_event_at))
        - unix_timestamp(opened_at)
    ) / 3600.0 as hours_open
from latest
