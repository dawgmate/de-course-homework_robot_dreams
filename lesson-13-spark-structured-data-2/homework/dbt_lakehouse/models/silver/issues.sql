
with parsed_events as (
    select
        event_id,
        event_type,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("issue_schema") }}') as parsed_payload
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

prepared as (
    select
        event_id,
        event_type,
        repo_name,
        parsed_payload.issue.number as issue_number,
        parsed_payload.issue.title as title,
        parsed_payload.issue.user.login as author_login,
        parsed_payload.issue.state as state,
        to_timestamp(
            parsed_payload.issue.created_at
        ) as opened_at,
        to_timestamp(
            parsed_payload.issue.closed_at
        ) as closed_at,
        parsed_payload.issue.comments as comments,
        transform(
            parsed_payload.issue.labels,
            label -> label.name
        ) as label_names,
        event_at as last_event_at
    from parsed_events
),

ranked as (
    select
        *,
        sum(
            case
                when event_type = 'IssueCommentEvent' then 1
                else 0
            end
        ) over (
            partition by repo_name, issue_number
        ) as comment_events_seen,
        row_number() over (
            partition by repo_name, issue_number
            order by last_event_at desc, event_id desc
        ) as row_num
    from prepared
)

select
    repo_name,
    issue_number,
    title,
    author_login,
    state,
    opened_at,
    closed_at,
    comments,
    label_names,
    comment_events_seen,
    last_event_at,
    case
        when closed_at is null then cast(null as double)
        else (
            unix_timestamp(closed_at)
            - unix_timestamp(opened_at)
        ) / 3600.0
    end as hours_to_close
from ranked
where row_num = 1
