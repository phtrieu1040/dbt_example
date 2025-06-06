from python_library.prefect_lib import Prefect
from clickhouse_connect import get_client
from typing import Literal
import pandas as pd
import polars as pl
import numpy as np
import datetime

class Clickhouse:
    def __init__(self):
        self.prefect = Prefect()
        self.client = None
        self.creds = None

    def get_clh_client(self):
        self.prefect.get_clickhouse_creds()
        creds = self.prefect.clickhouse_creds
        if creds is None:
            raise ValueError("Clickhouse credentials are not set. Please set the credentials.")
        self.client = get_client(
            host=creds["host"],
            port=creds["port"],
            user=creds["user"],
            password=creds["password"],
            secure=creds["secure"]
        )

    def _map_dtype(self, dtype):
        # Map pandas/polars dtype to ClickHouse type
        if pd.api.types.is_integer_dtype(dtype):
            return "Int64"
        elif pd.api.types.is_float_dtype(dtype):
            return "Float64"
        elif pd.api.types.is_bool_dtype(dtype):
            return "UInt8"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DateTime"
        else:
            return "String"

    def _get_schema_from_df(self, df):
        # Accepts pandas or polars DataFrame
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        schema = {}
        for col in df.columns:
            schema[col] = self._map_dtype(df[col].dtype)
        return schema

    def _create_table_if_not_exists(self, client, database, table_name, schema, partition_by=None, engine="MergeTree"):
        columns = ", ".join([f"{col} {col_type}" for col, col_type in schema.items()])
        if partition_by is not None:
            partition_by = f"PARTITION BY {partition_by}"
        else:
            partition_by = ""
        create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name} ({columns}) ENGINE = {engine} ORDER BY tuple() {partition_by}"""
        try:
            client.command(create_stmt)
        except Exception as e:
            raise ValueError(f"Error creating table {table_name}: {e}")

    def _insert_dataframe(self, client, database, table_name, df):
        client.insert(f"{database}.{table_name}", df)

    def _drop_table_if_exists(self, client, database, table_name):
        drop_stmt = f"DROP TABLE IF EXISTS {database}.{table_name}"
        try:
            client.command(drop_stmt)
        except Exception as e:
            raise ValueError(f"Error dropping table {table_name}: {e}")
    
    def _truncate_table(self, client, database, table_name):
        truncate_stmt = f"TRUNCATE TABLE {database}.{table_name}"
        try:
            client.command(truncate_stmt)
        except Exception as e:
            raise ValueError(f"Error truncating table {table_name}: {e}")

    def ingest_df(self, df, database, table_name, write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND", partition_by=None, engine="MergeTree"):
        self.get_clh_client()
        client = self.client
        schema = self._get_schema_from_df(df)

        # Drop table if exists
        if write_disposition == "WRITE_TRUNCATE":
            self._drop_table_if_exists(client, database, table_name)
        # Create table if not exists
        self._create_table_if_not_exists(client, database, table_name, schema, partition_by, engine)
        # Convert DataFrame to native Python types before insert
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        if isinstance(df, pd.DataFrame):
            df = df.where(pd.notnull(df), None)
            def to_native(x):
                if isinstance(x, (np.generic, np.ndarray)):
                    if isinstance(x, np.datetime64):
                        return pd.Timestamp(x)
                    return x.item()
                if isinstance(x, datetime.date) and not isinstance(x, datetime.datetime):
                    return pd.Timestamp(x)
                if isinstance(x, datetime.datetime):
                    return pd.Timestamp(x)
                return x
            df = df.applymap(to_native)
            # Ensure all datetime columns are of type datetime64[ns]
            for col in df.columns:
                # If column is already datetime64, or contains any datetime-like values, convert and fill
                if (
                    pd.api.types.is_datetime64_any_dtype(df[col])
                    or df[col].apply(lambda x: isinstance(x, (np.datetime64, datetime.date, datetime.datetime, pd.Timestamp))).any()
                ):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].fillna(pd.Timestamp("1970-01-01"))
        # Truncate if needed
        if write_disposition == "WRITE_TRUNCATE":
            self._truncate_table(client, database, table_name)
        # Insert data
        try:
            self._insert_dataframe(client, database, table_name, df)
            print(f"Dataframe inserted to table {database}.{table_name} successfully.")
            return True
        except Exception as e:
            raise ValueError(f"Error inserting dataframe to table {database}.{table_name}: {e}")
