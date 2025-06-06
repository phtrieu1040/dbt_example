from python_library.prefect_lib import Prefect
from python_library.google_lib import BigQuery
from python_library.clickhouse_lib import Clickhouse
from python_library.dbt_lib import Dbt
from prefect import flow, task
from pathlib import Path
from prefect_dbt.cli.commands import DbtCoreOperation

REPO_ROOT = Path(__file__).parent.parent
SQL_SCRIPTS_DIR = REPO_ROOT / "bqr_sql_script"

@task
def set_clickhouse_env_vars():
    prefect = Prefect()
    prefect.set_clickhouse_env_vars()

@task
def run_query_to_df():
    bqr = BigQuery()
    try:
        with open(SQL_SCRIPTS_DIR / "ops_sys__creator_payout_block.sql", "r") as file:
            query = file.read()
    except Exception as e:
        print(f"Error reading query file: {e}")
        return None
    print(query)
    try:
        df = bqr.query_to_df(query=query)
        print('query finished')
    except Exception as e:
        print(f"Error running query: {e}")
        return None
    print(df.head())
    return df

@flow
def write_df_to_clickhouse():
    df = run_query_to_df()
    if df is None:
        print("❌ No data to save")
        return
    clickhouse = Clickhouse()
    write_result = clickhouse.ingest_df(df, "tevi_data_team", "ops_sys__creator_payout_block_daily", write_disposition="WRITE_TRUNCATE", partition_by="toYYYYMM(ingest_at)", engine="MergeTree")
    if write_result:
        print("✅ Data ingested to ClickHouse successfully.")
    else:
        print("❌ Failed to ingest data to ClickHouse.")

@flow
def dbt_run_payout_block():
    dbt = Dbt()
    commands=["dbt run --select path:models/incremental_optimized/ops_sys__creator_payout_block_v2.sql"]
    result = dbt.run_dbt_command(commands, target="clickhouse")
    if result:
        print("✅ DBT run successfully.")
    else:
        print("❌ Failed to run DBT.")


@flow
def payout_block_clickhouse():
    write_df_to_clickhouse()
    dbt_run_payout_block()

if __name__ == "__main__":
    payout_block_clickhouse()