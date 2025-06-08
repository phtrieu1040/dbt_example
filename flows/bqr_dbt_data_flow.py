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
def set_env_vars():
    prf = Prefect()
    prf.set_clickhouse_env_vars()
    prf.set_bqr_env_vars()

@flow
def run_dbt_flow():
    normal_commands = [
        "dbt run --target bigquery --select tag:bqr"
    ]
    macro_commands = ["dbt run --target bigquery --operation test_operation"]
    full_commands = normal_commands
    full_commands.extend(macro_commands)
    for command in full_commands:
        print(command)
    dbt = Dbt()
    result = dbt.run_dbt_command(full_commands)
    return result

@flow
def bqr_dbt_data_flow():
    set_env_vars()
    run_dbt_flow()

if __name__ == "__main__":
    bqr_dbt_data_flow()