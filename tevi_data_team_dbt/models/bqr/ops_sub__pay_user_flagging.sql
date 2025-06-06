{{ config(
    cluster_by = ['user_id'],
    partition_by = none
) }}

with min_max as (
  select
    user_id,
    max(monthly_monetary_rank) max_rank_m,
    max(monthly_monetary_type) max_rank_name_m,
    min(monthly_monetary_rank) min_rank_m,
    min(monthly_monetary_type) min_rank_name_m,
    max(monthly_frequency_rank) max_rank_f,
    max(monthly_frequency_type) max_rank_name_f,
    min(monthly_frequency_rank) min_rank_f,
    min(monthly_frequency_type) min_rank_name_f,
  from {{ ref('ops_sub__monthly_pay_user_segmentation') }}
  group by all
)
, user_list as (
  select
    user_id,
    monthly_monetary_rank,
    monthly_monetary_type,
    monthly_frequency_rank,
    monthly_frequency_type,
    date_key as nearest_month,
    case
      when date_diff(date_trunc(current_date(), month), date_trunc(date_key, month), month) <= 3
        then 'recently'
      else 'long_ago'
    end as retire_type
  from {{ ref('ops_sub__monthly_pay_user_segmentation') }}
  qualify row_number() over(partition by user_id order by date_key desc) = 1
)
, latest as (
  select
    user_id,
    max(monthly_monetary_type) max_rank_name_m,
    max(monthly_monetary_rank) max_rank_m,
    max(monthly_frequency_type) max_rank_name_f,
    max(monthly_frequency_rank) max_rank_f,
  from {{ ref('ops_sub__monthly_pay_user_segmentation') }}
  where date_key >= date_sub(date_trunc(current_date(), month), interval 2 month)
  group by all
)
, with_pay_type as (
select
  ul.user_id,
  case
    when lt.max_rank_m is null then 'retired'
    else 'active'
  end as pay_user_type,
  coalesce(lt.max_rank_name_m, concat('Z. retired_', ul.retire_type)) as current_monetary_type,
  coalesce(lt.max_rank_name_f, concat('Z. retired_', ul.retire_type)) as current_frequency_type,
  coalesce(lt.max_rank_m, 0) as current_monetary_rank,
  coalesce(lt.max_rank_f, 0) as current_frequency_rank,
  coalesce(lt.max_rank_name_m, ul.monthly_monetary_type) as latest_active_monetary_type,
  coalesce(lt.max_rank_name_f, ul.monthly_frequency_type) as latest_active_frequency_type,
  ul.nearest_month as nearest_active_month,
from user_list as ul
left join latest as lt using(user_id)
left join min_max as mm using(user_id)
)
select * from with_pay_type