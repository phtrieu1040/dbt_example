{% macro test_macro() %}
    {{
        return(
            run_query("""
            create or replace table `tevi-data.tevi_db.test_bqr` as
            select current_date() as date_key
            """
            )
        ) 
    }}


{% endmacro %}