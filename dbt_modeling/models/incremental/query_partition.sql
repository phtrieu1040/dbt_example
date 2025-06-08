{{ config(
    partition_by = ["toStartOfMonth(created_at)"],
    engine = 'MergeTree',
    order_by = ['created_at']
) }}

select * 
from dataset.table_name
where true
    and toDate(created_at) >= toDate('{{ var("start_date") }}')
    and toDate(created_at) < toDate('{{ var("end_date") }}')