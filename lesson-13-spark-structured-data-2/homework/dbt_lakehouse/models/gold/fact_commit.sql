
select
    commit_sha,
    md5(repo_name) as repo_id,
    md5(pushed_by) as pusher_id,
    cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
    branch,
    is_merge_commit,
    is_distinct,
    message_length
from {{ ref('commits') }}
