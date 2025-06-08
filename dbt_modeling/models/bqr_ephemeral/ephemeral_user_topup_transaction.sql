select
    au.id as user_id,
    au.country,
    cast(cc.id as string) as id,
    null as seller_id,
    datetime(pp.updated_at) as first_purchase_at,
    case
        when coalesce(cc.top_up_quantity,0) = 0
            then cast(pp.amount as float64) * 100
        else cc.top_up_quantity
    end as amount,
    coalesce(json_extract_scalar(cc.lines, '$[0].conversion_package_slug'), pac.slug) as star_package,
    case
        when user_id like 'tevi_%' then 'internal_deposit'
        when gateway in ('appstore', 'google_play') then 'iap'
        when (
        gateway in ('gw.bank_transfer', 'gw.internal.tether', 'gw.internal.bank_transfer.vn')
        and user_id not like 'tevi_%'
        and top_up_quantity is not null
        ) then 'for_agency'
        else 'others'
    end as type,
from `tevi-data.paymee_tevi_paymee_db.checkout_checkout` as cc
left join `tevi-data.tevi_db.authentication_user` as au
    on cc.user_id = au.username
inner join `tevi-data.paymee_tevi_paymee_db.payment_payment` as pp
    on pp.checkout_id = cc.id
left join `tevi-data.paymee_tevi_paymee_db.tevi_stars_conversionpackage` as pac
    on cast(pac.id as string) = replace(json_value(to_json(pp.gateway_response), '$.productId'),'com.tevi.android.','')
where true
    and pp.charge_status in ('CHARGED', 'REFUNDED')
    and cc.status = 'COMPLETED'
    and cc.type = 'star'
    and coalesce(cc.is_test_purchase,false) = false
qualify
    sum(case when pp.charge_status = 'REFUNDED' then -1 else 1 end) over(partition by cc.id) = 1