{{ config(
    cluster_by = ['user_id', 'transaction_id']
) }}

with transfer_sellers as (
  select
    seller_id,
    min(purchase_at) as first_sell
  from {{ ref('bd_fact__star_purchase') }}
  where type = 'transfer_inbound'
  group by all
)
, transfer_inbound as (
  select sp.*
  from {{ ref('bd_fact__star_purchase') }} as sp
  left join transfer_sellers as ts
    on sp.buyer_id = ts.seller_id
    and sp.purchase_at < ts.first_sell
  where true
    and type != 'top_up'
    and ts.seller_id is null
    and sp.buyer_id is not null
)
, membership_donation as (
  select *
  from `tevi-data.tevi_data_team.ops_fact__total_event_v2`
  where product_type in ('stripe', 'donation_direct')
)
, unionized as (
  select
    date(first_purchase_at) as date_key,
    pt.user_id,
    'topup' as type,
    star_package as subtype,
    cast(tvs.id as string) as transaction_id,
    sum(pt.amount)/100 as usd_amount
  from {{ ref('ephemeral_user_topup_transaction') }} as pt
  left join `tevi_billy_db.billing_tvstransaction` as tvs
    on pt.id = tvs.paymee_order_id
  where true
    and not tvs.user_id like 'tevi%'
  group by all

  union all
  select
    purchase_at as date_key,
    buyer_id as user_id,
    'transfer_inbound' type,
    'transfer_inbound' subtype,
    cast(billing_transaction_id as string) as transaction_id,
    sum(amount)/100 as usd_amount
  from transfer_inbound
  group by all

  union all
  select
    transaction_date as date_key,
    user_id,
    case 
      when product_type = 'stripe' then 'membership_direct'
      else product_type
    end as type,
    product_type as subtype,
    consum_transaction_id as transaction_id,
    sum(usd_amount) as usd_amount
  from `tevi-data.tevi_data_team.ops_fact__total_event_v2`
  where product_type in ('stripe', 'donation_direct')
  group by all
)

select *
from unionized