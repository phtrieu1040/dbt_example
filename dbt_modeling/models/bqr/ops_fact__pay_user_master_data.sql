{{ config(
    cluster_by = ['user_id', 'transaction_id'],
) }}
with flagged as (
  select
    mpu.*,
    puw.monthly_usd_amount,
    puw.monthly_transaction_count,
    puw.monthly_monetary_type,
    puw.monthly_frequency_type,
    puw.monthly_monetary_rank,
    puw.monthly_frequency_rank,
    puf.nearest_active_month,
    puf.current_monetary_type,
    puf.current_frequency_type,
    puf.current_monetary_rank,
    puf.current_frequency_rank,
    puf.latest_active_monetary_type,
    puf.latest_active_frequency_type,
    puf.pay_user_type,
  from {{ ref('ops_sub__monthly_pay_user') }} as mpu
  left join {{ ref('ops_sub__monthly_pay_user_segmentation') }} as puw
    on mpu.user_id = puw.user_id
    and date_trunc(mpu.date_key, month) = puw.date_key
  left join {{ ref('ops_sub__pay_user_flagging') }} as puf
    on mpu.user_id = puf.user_id
)
select f.*,
  case
    when f.usd_amount <= 0.50 then 'N01. 0.00 - 0.50 usd'
    when f.usd_amount <= 1.00 then 'N02. 0.51 - 1.00 usd'
    when f.usd_amount <= 3.00 then 'N03. 1.01 - 3.00 usd'
    when f.usd_amount <= 5.00 then 'N04. 3.01 - 5.00 usd'
    when f.usd_amount <= 10.00 then 'N05. 5.01 - 10.00 usd'
    when f.usd_amount > 10.00 then 'N06. > 10.00 usd'
    else 'others'
  end as transaction_amount_group,
  um.user_alias,
  um.suspended_at,
  um.suspend_reason,
  um.country as user_country_code,
  um.email_domain,
  um.provider,
  um.preferred_os_system,
  um.is_suspicious_user,
  coalesce(um.is_sus_social or um.is_suspicious_x_social) as is_suspicious_social,
  um.analytics_user_contry_code,
from flagged as f
left join `tevi-data.tevi_data_team.ops_dim__user_master_data` as um using(user_id)