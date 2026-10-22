
select
    md5(
        concat_ws(
            '|',
            repo_name,
            cast(pr_number as string)
        )
    ) as pr_id,
    md5(repo_name) as repo_id,
    md5(author_login) as author_id,
    cast(date_format(opened_at, 'yyyyMMdd') as int) as opened_date_id,
    case
        when merged_at is null then cast(null as int)
        else cast(date_format(merged_at, 'yyyyMMdd') as int)
    end as merged_date_id,
    state,
    is_merged,
    is_draft,
    additions,
    deletions,
    churn,
    changed_files,
    commits_count,
    comments,
    review_comments,
    hours_open,
    case
        when label_names is null then 0
        else size(label_names)
    end as label_count
from {{ ref('pull_requests') }}
