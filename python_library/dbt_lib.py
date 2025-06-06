from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow, task
from python_library.prefect_lib import Prefect
from python_library.utility_lib import DateAndTime
from clickhouse_connect import get_client
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


class Dbt:
    def __init__(self):
        self.prefect = Prefect()

    def _set_env_vars(self):
        self.prefect.set_clickhouse_env_vars()
    
    def run_dbt_command(self, commands, target="clickhouse"):
        self._set_env_vars()
        result = None
        if target not in ["bigquery", "clickhouse"]:
            print(f"Invalid target: {target}")
            return None
        # Assume commands is a list of base dbt commands (strings)
        full_commands = []
        for cmd in commands:
            if "--target" not in cmd:
                cmd += f" --target {target}"
            full_commands.append(cmd)
        try:
            result = DbtCoreOperation(
                commands=full_commands,
                project_dir="tevi_data_team_dbt",
                profiles_dir="tevi_data_team_dbt"
            ).run()
        except Exception as e:
            print(f"Error running dbt command: {e}")
        return result
    
    def _generate_commands_for_partition_table(self, model_path, backfill=False, target="clickhouse"):
        if backfill:
            start_date = "2022-01-01"
        else:
            start_date = (datetime.today().replace(day=1) - relativedelta(months=4)).strftime("%Y-%m-%d")

        end_date = datetime.today().replace(day=1) + relativedelta(months=1)
        date_list = DateAndTime.generate_date_list(start_date=start_date, end_date=end_date, interval=1, interval_type="months")
        full_commands = []
        for i in range(len(date_list) - 1):
            start = date_list[i].strftime("%Y-%m-%d")
            end = date_list[i+1].strftime("%Y-%m-%d")
            command_line = f"dbt run --target {target} --select path:{model_path} --vars '{{start_date: \"{start}\", end_date: \"{end}\"}}'"
            full_commands.append(command_line)
        return full_commands