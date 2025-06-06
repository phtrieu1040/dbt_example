{{ config(
    partition_by = ["toStartOfMonth(created_at)"],
    engine = 'MergeTree',
    order_by = ['created_at']
) }}

select * 
from tevi_billy.billing_tvstransaction
where true
    and toDate(created_at) >= toDate('{{ var("start_date") }}')
    and toDate(created_at) < toDate('{{ var("end_date") }}')