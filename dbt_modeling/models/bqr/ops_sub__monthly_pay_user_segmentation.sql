{{ config(
    cluster_by = ['user_id'],
) }}

with monthly_data as (
  select 
    user_id,
    date_trunc(date_key, month) as date_key,
    sum(usd_amount) usd_amount,
    count(distinct transaction_id) as transaction_count
  from {{ ref('ops_sub__monthly_pay_user') }} as u
  group by all
)
, segmentation as (
  select
    *,
    case
      when transaction_count = 1 and usd_amount <= 0.5 then 'N01. one_timer'
      when usd_amount <= 1 then 'N02. micro_user'
      when usd_amount <= 2 then 'N03. light_user'
      when usd_amount <= 5 then 'N04. casual_user'
      when usd_amount <= 10 then 'N05. power_user'
      when usd_amount > 10 then 'N06. whale_user'
      else 'others'
    end as monthly_monetary_type,

    
    case
      when transaction_count = 1 and usd_amount <= 0.5 then 1
      when usd_amount <= 1 then 2
      when usd_amount <= 2 then 3
      when usd_amount <= 5 then 4
      when usd_amount <= 10 then 5
      when usd_amount > 10 then 6
      else 0
    end as monthly_monetary_rank,

    case
      when transaction_count = 1 and usd_amount <= 0.5 then 'N01. tester'
      when transaction_count = 1 then 'N02. one_timer'
      when transaction_count <= 3 then 'N03. occational_user'
      when transaction_count <= 7 then 'N04. engaged_user'
      when transaction_count <= 10 then 'N05. regular_user'
      when transaction_count > 10 then 'N06. enthusiastic_user'
      else 'others'
    end as monthly_frequency_type,
    case
      when transaction_count = 1 and usd_amount <= 0.5 then 1
      when transaction_count = 1 then 2
      when transaction_count <= 3 then 3
      when transaction_count <= 7 then 4
      when transaction_count <= 10 then 5
      when transaction_count > 10 then 6
      else 0
    end as monthly_frequency_rank,
  from monthly_data
)
, with_segmentation as (
  select distinct
    date_trunc(u.date_key, month) as date_key,
    u.user_id,
    spp.usd_amount as monthly_usd_amount,
    spp.transaction_count as monthly_transaction_count,
    spp.monthly_monetary_type,
    spp.monthly_monetary_rank,
    spp.monthly_frequency_type,
    spp.monthly_frequency_rank
  from {{ ref('ops_sub__monthly_pay_user') }} as u
  left join segmentation as spp
    on u.user_id = spp.user_id
    and date_trunc(u.date_key, month) = spp.date_key
)
select *
from with_segmentation