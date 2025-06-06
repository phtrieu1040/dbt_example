{{ config(materialized='table', schema='tevi_analytics_db') }}
SELECT
    id,
    username,
    date_joined
FROM tevi_core.authentication_user
WHERE date_joined >= '2024-01-01'