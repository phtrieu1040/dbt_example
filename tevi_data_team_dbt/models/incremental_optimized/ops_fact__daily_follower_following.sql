{{
    config(
        unique_key = ['owner_id', 'follower_id', 'date_key'],
        engine = 'ReplacingMergeTree',
        order_by = ['owner_id', 'follower_id', 'date_key', 'unfollowed_at'],
        partition_by = ["toStartOfMonth(date_key)"]
    )
}}

{% if is_incremental() %}

with insert_records as (
    select
        coalesce(lf.date_key, t.date_key) as date_key,
        coalesce(lf.owner_id, t.owner_id) as owner_id,
        coalesce(lf.follower_id, t.follower_id) as follower_id,
        coalesce(lf.followed_at, t.followed_at) as followed_at,
        case
            when lf.owner_id is null then toDateTime(now())
            else toDateTime('1970-01-01 00:00:00')
        end as unfollowed_at,
        case
            when lf.owner_id is null then false
            else true
        end as following_status,
        coalesce(lf.is_auto_follow, t.is_auto_follow) as is_auto_follow,
        case
            when lf.owner_id is null then 'unfollow'
            else 'follow'
        end as note
    from {{ this }} as t
    full join {{ ref('ephemeral_latest_follow') }} as lf
        on t.owner_id = lf.owner_id
        and t.follower_id = lf.follower_id
        and t.date_key = lf.date_key
    where 1=1
        and coalesce(lf.following_status, false) != coalesce(t.following_status, false)
)

select *
from insert_records
where 1=1
    and (
        date_key >= date_sub(toDate(now()), 3)
        or toDate(unfollowed_at) >= date_sub(toDate(now()), 3)
    )
qualify row_number() over(partition by owner_id, follower_id order by date_key desc) = 1

{% else %}

select * from {{ ref('ephemeral_latest_follow') }}

{% endif %}

