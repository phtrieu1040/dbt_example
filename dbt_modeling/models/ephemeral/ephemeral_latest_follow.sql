select
    toDate(cf.created_at) as date_key,
    ch.owner_id,
    cf.follower_id,
    cf.created_at as followed_at,
    toDateTime('1970-01-01 00:00:00') as unfollowed_at,
    true as following_status,
    coalesce(au.auto_follow,0) = 1 as is_auto_follow,
    'follow' as note
from tevi_core.channel_channelfollow as cf
left join tevi_core.channel_channel as ch
    on cf.channel_id = ch.id
left join tevi_core.authentication_user as au
    on cf.follower_id = au.id
where 1=1
    and cf.created_at >= toDate(now()) - interval 1 month
qualify row_number() over(partition by ch.owner_id, cf.follower_id order by cf.created_at desc) = 1