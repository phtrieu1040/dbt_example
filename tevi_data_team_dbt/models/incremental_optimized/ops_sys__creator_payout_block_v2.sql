{{ config(
    partition_by = "toStartOfMonth(ingest_at)",
    engine = 'ReplacingMergeTree(ingest_at)',
    order_by = ['creator_id', 'ingest_cycle'],
    unique_key = ['creator_id', 'ingest_cycle']
) }}


{% if is_incremental() %}

select *
from tevi_data_team.ops_sys__creator_payout_block_daily
where true

{% else %}

select
    ingest_at,
    formatDateTime(ingest_at, '%Y-%V') as ingest_cycle,
    creator_id,
    creator_alias,
    payout_score
from tevi_data_team.ops_sys__creator_payout_block
where true


{% endif %}