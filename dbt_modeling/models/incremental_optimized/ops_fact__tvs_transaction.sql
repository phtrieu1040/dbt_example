{{ config(
    partition_by = ["toStartOfMonth(transaction_datetime)"],
    engine = 'ReplacingMergeTree(transaction_datetime)',
    order_by = ['consum_transaction_id'],
    unique_key = ['consum_transaction_id']
) }}

select
    toDate(t.created_at) as transaction_date,
    t.created_at as transaction_datetime,
    t.order_id,
    t.user_id,
    toString(t.id) as consum_transaction_id, -- from user to creator
    sum(t.amount / -100) as usd_amount
from tevi_billy.billing_tvstransaction as t final
where 1=1
    and toDate(created_at) >= toDate('{{ var("start_date") }}')
    and toDate(created_at) < toDate('{{ var("end_date") }}')
group by all