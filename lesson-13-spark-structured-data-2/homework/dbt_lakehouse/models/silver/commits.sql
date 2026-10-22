
with parsed_events as (
    select
        event_id,
        repo_name,
        actor_login,
        created_at,
        from_json(payload, '{{ var("push_schema") }}') as parsed_payload
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),

exploded_commits as (
    select
        event_id,
        repo_name,
        actor_login as pushed_by,
        created_at as pushed_at,
        parsed_payload.ref as git_ref,
        explode(parsed_payload.commits) as commit
    from parsed_events
),

prepared as (
    select
        commit.sha as commit_sha,
        repo_name,
        pushed_by,
        regexp_replace(git_ref, '^refs/heads/', '') as branch,
        commit.author.name as author_name,
        commit.author.email as author_email,
        commit.message as message,
        commit.`distinct` as is_distinct,
        pushed_at,
        commit.message like 'Merge %' as is_merge_commit,
        element_at(split(commit.message, '\n'), 1) as message_subject,
        length(commit.message) as message_length,
        event_id
    from exploded_commits
),

ranked as (
    select
        *,
        row_number() over (
            partition by commit_sha
            order by pushed_at asc, event_id asc
        ) as row_num
    from prepared
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from ranked
where row_num = 1
