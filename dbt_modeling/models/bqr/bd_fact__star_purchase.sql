{{ config(
    partition_by = {
      'field' : 'purchase_at',
      'data_type' : 'date'
    }
) }}

with user_with_keyword_post as (
  select distinct
    p.owner_id as user_id,
    max(regexp_contains(lower(p.text), r'(?i)\bseller\b|\bidr\b|\bprice\b|\bcontact\b')) as is_keyword_contains
  from `tevi-data.tevi_db.post` as p
  group by all
)

, final as (
  select
    date(btv.created_at) as purchase_at,
    datetime(btv.created_at) as purchase_time,
    au2.id as buyer_id,
    coalesce(au2.alias, cast(au2.prev_alias as int64)) as buyer_alias,
    au2.display_name as buyer_display_name,
    concat('https://tevi.com/@', ch2.slug) as buyer_page,
    au2.country as buyer_country,
    coco2.name as buyer_country_name,
    au.id as seller_id,
    coalesce(au.alias, cast(au.prev_alias as int64)) as seller_alias,
    au.display_name as seller_display_name,
    concat('https://tevi.com/@', ch.slug) as seller_page,
    case when btv.type = 'top_up' then null else au.country end as seller_country,
    case when btv.type = 'top_up' then null else coco.name end as seller_country_name,
    coalesce(dim.is_allowed, false) as buyer_is_allowed_transfer,
    coalesce(dim2.is_allowed, false) as seller_is_allowed_transfer,
    pp.gateway,
    case
        when cc.user_id like 'tevi_%' then 'internal_deposit'
        when pp.gateway in ('appstore', 'google_play') then 'iap'
        when (
          pp.gateway in ('gw.bank_transfer', 'gw.internal.tether', 'gw.internal.bank_transfer.vn')
          and cc.user_id not like 'tevi_%'
          and cc.top_up_quantity is not null
        ) then 'for_agency'
        when btv.type = 'transfer_inbound' then 'transfer_inbound'
        else 'others'
    end as topup_type,
    btv.id as billing_transaction_id,
    cast(cc.id as string) as checkout_id,
    coalesce(json_extract_scalar(cc.lines, '$[0].conversion_package_slug'), pac.slug) as topup_star_package,
    pp.charge_status,
    case when pp.charge_status = 'REFUNDED' then -btv.amount else btv.amount end as amount,
    case
      when sum(case when pp.charge_status = 'REFUNDED' then 1 else 0 end) over(partition by cc.id) = 0
        then false
      else true
    end as is_refunded,
    btv.type,
    coalesce(uk.is_keyword_contains and dim2.is_allowed, false) as is_keyword_contains

  from `tevi-data.tevi_billy_db.billing_tvstransaction` as btv
  left join `tevi-data.tevi_db.authentication_user` as au
    on json_extract_scalar(btv.metadata, '$.sender_alias') = coalesce(cast(au.alias as string), au.prev_alias)
  left join `tevi-data.tevi_db.authentication_user` as au2
    on btv.user_id = au2.username
  left join `tevi-data.tevi_data_team.ops_dim__star_transfer` as dim
    on au2.id = dim.user_id
  left join `tevi-data.tevi_data_team.ops_dim__star_transfer` as dim2
    on au.id = dim2.user_id
  left join `tevi-data.tevi_db.channel_channel` as ch
    on au.id = ch.owner_id
  left join `tevi-data.tevi_db.channel_channel` as ch2
    on au2.id = ch2.owner_id
  left join `tevi-data.paymee_tevi_paymee_db.checkout_checkout` as cc
    on btv.paymee_order_id = cc.id
  left join `tevi-data.paymee_tevi_paymee_db.payment_payment` as pp
    on pp.checkout_id = cc.id
  left join `tevi-data.paymee_tevi_paymee_db.tevi_stars_conversionpackage` as pac
    on cast(pac.id as string) = replace(json_value(to_json(pp.gateway_response), '$.productId'),'com.tevi.android.','')
  left join user_with_keyword_post as uk
    on au.id = uk.user_id
  left join `tevi-data.tevi_billy_db.common_country` as coco
    on au.country = coco.alpha_2
  left join `tevi-data.tevi_billy_db.common_country` as coco2
    on au2.country = coco2.alpha_2
  where true
    and btv.type in ('top_up', 'transfer_inbound')
    -- and (pp.id is null or pp.charge_status = 'CHARGED')
)
select *
from final
where true
  and not (type = 'transfer_inbound' and seller_alias is null)