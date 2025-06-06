from prefect import flow, task, get_run_logger
from python_library.prefect_lib import Prefect
from python_library.google_lib import BigQuery
from python_library.clickhouse_lib import Clickhouse
from python_library.dbt_lib import Dbt
from prefect_dbt.cli.commands import DbtCoreOperation
import os
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SQL_SCRIPTS_DIR = REPO_ROOT / "bqr_sql_script"

@task
def set_clickhouse_env_vars():
    prf = Prefect()
    prf.set_clickhouse_env_vars()
    prf.set_bqr_env_vars()

# @task
# def generate_commands(commands):
#     dbt = Dbt()
#     tvs_commands = dbt._generate_commands_for_partition_table(backfill=False, model_path="models/incremental_optimized/ops_fact__tvs_transaction.sql")
#     full_commands = []
#     full_commands.extend(commands)
#     full_commands.extend(tvs_commands)
#     return full_commands

@flow
def run_dbt_flow():
    normal_commands = [
        "dbt run --target bigquery --select tag:bqr",
        "dbt run --target clickhouse --select path:models/normal/ops_fact__user_indo_push_noti.sql",
    ]
    macro_commands = []
    # full_commands = generate_commands(normal_commands)
    full_commands = normal_commands
    full_commands.extend(macro_commands)
    for command in full_commands:
        print(command)
    dbt = Dbt()
    result = dbt.run_dbt_command(full_commands)
    return result

@flow
def tevi_data_dbt_flow():
    set_clickhouse_env_vars()
    run_dbt_flow()

if __name__ == "__main__":
    tevi_data_dbt_flow()