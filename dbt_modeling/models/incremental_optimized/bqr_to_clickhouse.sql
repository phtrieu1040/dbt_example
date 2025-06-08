{{ config(
    partition_by = "toStartOfMonth(ingest_at)",
    engine = 'ReplacingMergeTree(ingest_at)',
    order_by = ['creator_id', 'ingest_cycle'],
    unique_key = ['creator_id', 'ingest_cycle']
) }}


{% if is_incremental() %}

select *
from dataset.table_name
where true

{% else %}

select *
from dataset.table_name
where true


{% endif %}