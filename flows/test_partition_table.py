from prefect import flow, task
from prefect_dbt.cli.commands import DbtCoreOperation
from python_library.prefect_lib import Prefect
from python_library.utility_lib import DateAndTime
from clickhouse_connect import get_client
import os
@task
def generate_commands():
    # Set your desired date range
    date_list = DateAndTime.generate_date_list(start_date="2025-01-01", end_date="2025-06-01", interval=1, interval_type="months")
    command_list = []
    for i in range(len(date_list) - 1):
        start = date_list[i].strftime("%Y-%m-%d")
        end = date_list[i+1].strftime("%Y-%m-%d")
        command_line = f"dbt run --target clickhouse --select path:models/incremental/tevi_billy__billing_tvstransaction.sql --vars '{{start_date: \"{start}\", end_date: \"{end}\"}}'"
        command_list.append(command_line)
    return command_list

@task
def set_clickhouse_env_vars():
    prf = Prefect()
    prf.set_clickhouse_env_vars()

@task
def get_clickhouse_client():
    client = get_client(host=os.getenv("CLICKHOUSE_HOST"), port=os.getenv("CLICKHOUSE_PORT"), user=os.getenv("CLICKHOUSE_USER"), password=os.getenv("CLICKHOUSE_PASSWORD"))
    return client

@flow
def pre_hook():
    client = get_clickhouse_client()
    client.command("drop table if exists tevi_data_team.tevi_billy__billing_tvstransaction")
    print("table dropped")

@flow
def post_hook():
    client = get_clickhouse_client()
    client.command("create materialized view if not exists tevi_data_team.mv__tevi_billy__billing_tvstransaction to tevi_data_team.tevi_billy__billing_tvstransaction as select * from tevi_billy.billing_tvstransaction")
    print("mv created")

@flow
def test_partition_table():
    commands = generate_commands()
    for command in commands:
        print(command)
    result = DbtCoreOperation(
        commands=commands,
        project_dir="tevi_data_team_dbt",
        profiles_dir="tevi_data_team_dbt"
    ).run()
    print("dbt run completed")
    print(result)

@flow
def test_partition_table_flow():
    set_clickhouse_env_vars()
    print(os.getenv("CLICKHOUSE_HOST"))
    print(os.getenv("CLICKHOUSE_PORT"))
    print(os.getenv("CLICKHOUSE_USER"))
    print(os.getenv("CLICKHOUSE_PASSWORD"))
    result = DbtCoreOperation(
        commands=["dbt run --target clickhouse --select path:models/incremental_optimized/ops_sys__creator_payout_block_v2.sql"],
        project_dir="tevi_data_team_dbt",
        profiles_dir="tevi_data_team_dbt",
        env={"CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST"), "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT"), "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"), "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD")}
    ).run()
    print(result)
    pre_hook()
    test_partition_table()
    # post_hook()

if __name__ == "__main__":
    test_partition_table_flow()

