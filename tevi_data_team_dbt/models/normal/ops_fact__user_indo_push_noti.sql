{{ config(
    engine = 'MergeTree',
    order_by = ['created_at'],
    partition_by = 'toDate(created_at)'
) }}

select
    distinct_id,
    created_at,
    event,
    uuid,
    JSONExtractString(properties, '$os') AS os,
    JSONExtractString(properties, 'utm_campaign') AS utm_campaign,
    JSONExtractString(properties, '$browser') AS browser,
    JSONExtractString(properties, '$geoip_country_name') AS country_name,
    JSONExtractString(properties, '$ip') AS ip,
    JSONExtractString(properties, 'distinct_id') AS distinct_id_properties,
    JSONExtractString(properties, '$browser_version') AS browser_version,
    JSONExtractString(properties, '$device') AS device,
    JSONExtractString(properties, '$os_version') AS os_version,
    JSONExtractString(properties, '$raw_user_agent') AS raw_user_agent,
    JSONExtractString(properties, '$session_id') AS session_id,
    JSONExtractString(properties, '$browser_language') AS browser_language,
    $session_id,
    $window_id,
    $session_id_uuid,
    person_mode
from posthog.events
where toDate(created_at) >= toDate('2025-02-06')
  and properties like '%apk_push_user_indo%'