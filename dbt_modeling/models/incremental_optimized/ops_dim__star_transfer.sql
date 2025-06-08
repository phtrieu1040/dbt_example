{{
    config(
        unique_key = ['user_id'],
        engine = 'ReplacingMergeTree',
        order_by = ['user_id', 'created_at'],
        partition_by = ["toStartOfMonth(created_at)"]
    )
}}

{% if is_incremental() %}
with insert_records as (
    select
        coalesce(t.user_id, lf.user_id) as user_id,
        coalesce(t.alias, lf.alias) as alias,
        case
            when t.user_id is not null and lf.user_id is not null
                then coalesce(t.is_allowed, lf.is_allowed)
            else case
                when lf.user_id is null then false
                else coalesce(lf.is_allowed, false)
            end
        end as is_allowed,
        coalesce(t.created_at, lf.created_at) as created_at,
        coalesce(lf.last_updated_at, t.last_updated_at) as last_updated_at,
        case
            when t.user_id is not null and lf.user_id is not null
                then 'as_is'
            else case
                    when t.user_id is null then 'insert'
                    else 'update'
        end as operation
    from {{ this }} as t
    full join {{ ref('ephemeral_latest_star_transfer') }} as lf
        on t.user_id = lf.user_id
    where 1=1
)

select * except(operation)
from insert_records
where 1=1
    and (
        created_at >= date_sub(toDate(now()), 3)
        or toDateTime(last_updated_at) >= date_sub(toDate(now()), 3)
    )
    and operation != 'as_is'
qualify row_number() over(partition by user_id order by created_at desc) = 1

{% else %}

select * from {{ ref('ephemeral_latest_star_transfer') }}

{% endif %}