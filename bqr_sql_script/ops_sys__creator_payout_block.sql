WITH lv_date AS (
  SELECT
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATETIME()) = 1 THEN CURRENT_DATE()
      ELSE CURRENT_DATE() - 1
    END AS lv_date
),
cte AS (
  SELECT DISTINCT
    CURRENT_DATETIME() AS ingest_at,
    FORMAT_DATETIME('%Y-%V', DATE_SUB(lv.lv_date, INTERVAL 1 WEEK)) AS ingest_cycle,
    creator_id,
    creator_alias,
    CASE
      WHEN cheat_index >= 0.5
           OR conversion_rate >= 0.8
           OR concentration_index >= 0.5
           OR concentration_index <= 0.01
           OR is_sus_social THEN 1
      WHEN cheat_index >= 0.2
           OR conversion_rate >= 0.5
           OR concentration_index >= 0.1
           OR concentration_index <= 0.03
           OR is_sus_device
           OR is_sus_ip THEN 2
      ELSE 0
    END AS payout_score
  FROM `tevi-data.tevi_data_team.ops_fact__creator_master_data_v3` cm
  LEFT JOIN `tevi-data.tevi_data_team.ops_dim__user_master_data` um
    ON cm.creator_id = um.user_id
  ,lv_date as lv
)
SELECT * FROM cte WHERE payout_score = 1