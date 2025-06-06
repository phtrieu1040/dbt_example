select
    toDate(now()) as created_at,
    au.id as user_id,
    vst.alias as alias,
    true as is_allowed,
    toDate(now()) as last_updated_at
from tevi_core.authentication_user as au
left join tevi_core.vw_dim__star_transfer as vst
    on cast(vst.alias as string) = coalesce(cast(au.alias as string), au.prev_alias)
where 1=1
    and vst.alias is not null