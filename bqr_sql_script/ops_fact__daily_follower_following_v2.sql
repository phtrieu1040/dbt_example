select * except(unfollowed_at, note)
from `tevi-data.tevi_data_team.ops_fact__daily_follower_following_v2`
where unfollowed_at is not null
limit 1000