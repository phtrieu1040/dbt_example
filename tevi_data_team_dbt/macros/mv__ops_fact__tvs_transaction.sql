{% macro mv__ops_fact__tvs_transaction() %}
    {{ run_query("optimize table tevi_billy.billing_tvstransaction final") }}

    {{
        run_query("""
        create materialized view if not exists tevi_data_team.mv__ops_fact__tvs_transaction
        to tevi_data_team.ops_fact__tvs_transaction
        as 
        select
            toDate(t.created_at) as transaction_date,
            t.created_at as transaction_datetime,
            t.order_id,
            t.user_id,
            toString(t.id) as consum_transaction_id, -- from user to creator
            sum(t.amount / -100) as usd_amount
        from tevi_billy.billing_tvstransaction as t
        where 1=1
            and toDate(created_at) >= toDate(now()) - interval 1 month
            and toDate(created_at) < toDate(now())
        group by all
        """
        )
    }}
{% endmacro %}