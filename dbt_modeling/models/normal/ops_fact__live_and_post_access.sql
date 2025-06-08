{{ config(
    materialized = 'table',
    engine = 'ReplacingMergeTree',
    order_by = ['transaction_date', 'revenue_transaction_id'],
    partition_by = ['transaction_date']
) }}

select
    toDate(t.created_at) as transaction_date,
    t.created_at as transaction_datetime,
    coalesce(eo.event_code, bt.event_code) as event_code,
    t.order_id,
    eo.product_id,
    CAST(t.id AS String) as consum_transaction_id, -- from user to creator
    bt.id as revenue_transaction_id, -- creator receive
    ep.type,
    ep.subtype,
    au2.id as user_id,
    sum(t.amount / -100) as usd_amount
from tevi_billy.billing_tvstransaction as t
left join tevi_billy.ecom_order as eo
    on t.order_id = eo.id
left join tevi_billy.ecom_product as ep
    on ep.id = eo.product_id
left join tevi_billy.billing_transaction as bt
    on bt.id = eo.billing_transaction_id
left join tevi_core.authentication_user as au
    on bt.user_id = au.username
left join tevi_core.authentication_user as au2
    on t.user_id = au2.username
where t.type = 'consumption'
  and ep.type = 'asset'
  and t.created_at >= date_sub(toDate(now()), 1)
group by all
limit 1
