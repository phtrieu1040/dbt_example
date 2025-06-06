from .auth_and_token import GoogleAuthManager
from .utility_lib import MyFunction
from typing import Literal
import datetime as lib_dt
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
import pandas as pd


class BigqueryQuery:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = client_secret_directory
        self.bqr_client = None
        self.bqr_cred = None

    def _get_bqr_client(self):
        self.auth_manager.check_cred()
        self.bqr_client = self.auth_manager.credentials.client

    def _get_bqr_cred(self):
        self.auth_manager.check_cred()
        self.bqr_cred = self.auth_manager.credentials

    def list_all_scheduled_queries(self, location):
        self._get_bqr_client()
        self._get_bqr_cred()
        client = self.bqr_client
        cred = self.bqr_cred
        project_id = client.project
        transfer_client = bigquery_datatransfer.DataTransferServiceClient(credentials=cred)
        parent = f"projects/{project_id}/locations/{location}"

        configs = transfer_client.list_transfer_configs(parent=parent)
        scheduled_queries = []
        for config in configs:
            if config.data_source_id == 'scheduled_query':
                scheduled_queries.append({
                    'display_name': config.display_name,
                    'sql': config.params.get('query'),
                    'destination_table': config.params.get('destination_table_name_template'),
                    'schedule': config.schedule,
                    'config_name': config.name,
                    'project_id': project_id,
                    'location': location
                })

        df = pd.DataFrame(scheduled_queries)
        return df
        # return cred, project_id

    def bigquery_operation(self, query=None, sql_file_path=None):
        self._get_bqr_client()
        client = self.bqr_client
        udf=MyFunction()
        if sql_file_path:
            query_string = udf._read_sql(sql_file_path=sql_file_path)
        else:
            query_string = query
        try:
            query_job = client.query(query_string)
            query_job.result()
        except Exception as e:
            print('Error running query: ',e)
            return False
        return True

    def run_biqquery_to_df(self, query=None, sql_file_path=None):
        udf=MyFunction()
        if sql_file_path:
            query_string = udf._read_sql(sql_file_path=sql_file_path)
        else:
            query_string = query

        self._get_bqr_client()
        client = self.bqr_client
        try:
            query_job = client.query(query_string)
            query_job.result()
            df = query_job.to_dataframe()
            return df
        except Exception as e:
            print('Error while fetching data to df: ', e)

class BigqueryTable:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = client_secret_directory
        self.bqr_client = None
        self.bqr_cred = None

    def _get_bqr_client(self):
        self.auth_manager.check_cred()
        self.bqr_client = self.auth_manager.credentials.client

    def _set_expiration_to_table(self, table_id, expiration_day):
        self._get_bqr_client()
        client = self.bqr_client
        table = client.get_table(table_id)
        table.expires = lib_dt.datetime.now() + lib_dt.timedelta(days=expiration_day)
        client.update_table(table, ['expires'])

    def create_ingestion_time_partitioned_table(self, table_id):
        # once the table is created, write append to it, 
        from google.cloud.bigquery import Table, TimePartitioning, TimePartitioningType
        self._get_bqr_client()
        client = self.bqr_client
        table = Table(table_id)
        table.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY) # must only be day for ingestion time table, can't be any other.
        table.require_partition_filter = True
        table = client.create_table(table)

    def _enable_partition_filter(self, table_id):
        self._get_bqr_client()
        client = self.bqr_client
        table = client.get_table(table_id)  # Make an API request to fetch the table.
        table.require_partition_filter = True  # Set the partition filter requirement.
        client.update_table(table, ["require_partition_filter"])  # Make an API request to update the table.

    def update_table_from_dataframe(
            self, df, table_id,
            write_disposition='WRITE_TRUNCATE',
            partition_column_name=None,
            partition_column_type=None,
            table_expiration='',
            partition_expiration='',
            partition_filter=False
            ):
        if partition_column_name and partition_column_type:
            check_time_partition = True
        else:
            check_time_partition = False
        if check_time_partition==False:
            table = self._update_table_from_dataframe_no_partition(df,table_id,write_disposition)
        else:
            if write_disposition == 'WRITE_TRUNCATE_PARTITION':
                table = self._update_table_from_dataframe_partititon(df,table_id, partition_column_name, partition_column_type)
            else:
                table = self._update_table_from_dataframe_other(df,table_id,write_disposition, partition_column_name, partition_column_type)
        
        if table_expiration:
            try:
                self._set_expiration_to_table(table_id=table_id, expiration_day=table_expiration)
                print('Successfully set expiration to table')
            except Exception as e:
                print('Error setting expiration date to table: ', e)
        else:
            pass

        try:
            self._get_bqr_client()
            client = self.bqr_client
            table = client.get_table(table_id)
            is_partition_filter_required = table.require_partition_filter
        except Exception as e:
            print('Error checking if table is required for partition filter: ', e)
            is_partition_filter_required = False

        if (is_partition_filter_required == True and partition_expiration) or (partition_expiration and check_time_partition):
            try:
                self._create_expiration_for_partition_for_table(table_id=table_id, expiration=partition_expiration)
                if partition_filter: self._enable_partition_filter(table_id=table_id)
                print('Successfully set expiration to partition')
            except Exception as e:
                print('Error create expiration for partition: ', e)
        else:
            pass

        return table

    def _update_table_from_dataframe_no_partition(self, df, table_id, write_disposition):
        table = table_id
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition,
                                            autodetect=True)
        job = self._get_bqr_client().load_table_from_dataframe(df, table, job_config=job_config)

        job.result()
        self._get_bqr_client()
        client = self.bqr_client
        table = client.get_table(table)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        return table

    def _update_table_from_dataframe_other(self,df,table_id,write_disposition, partition_column_name, partition_column_type):
        # field_partition = time_partitioning['field']
        # type_partition = time_partitioning['type']
        field_partition = partition_column_name
        type_partition = partition_column_type
        table = table_id
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition,
                                            time_partitioning=bigquery.TimePartitioning(
                                                type_=type_partition,
                                                field=field_partition,  # field to use for partitioning
                                                ),
                                            autodetect=True)
        self._get_bqr_client()
        client = self.bqr_client
        job = client.load_table_from_dataframe(df,table,job_config=job_config)

        job.result()
        table = self._get_bqr_client().get_table(table)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        return table
    
    def _write_truncate_whole_table(self, df, table_id):
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                            autodetect=True)
        self._get_bqr_client()
        client = self.bqr_client
        client.load_table_from_dataframe(dataframe=df,
                                         destination=table_id,
                                         job_config=job_config)
        
    def _create_expiration_for_partition_for_table(self, table_id, expiration=None):
        # partition with expiration mean once the expiration period is reach, all rows in the table with the expired partitions will be removed
        if expiration:
            try:
                partition_expiration_ms = expiration * 24 * 60 * 60 * 1000 # 30 days in milliseconds
            except Exception as e:
                print('Error create partition for table: ', e)
        else:
            partition_expiration_ms = expiration
            # return
        self._get_bqr_client()
        client = self.bqr_client
        table = client.get_table(table_id)
        if not table.time_partitioning:
            from google.cloud.bigquery import TimePartitioning
            table.time_partitioning = TimePartitioning(expiration_ms=partition_expiration_ms)
        else:
            pass
            # table.time_partitioning.expiration_ms = partition_expiration_ms
        
        table.time_partitioning.expiration_ms = partition_expiration_ms 
        try:
            client.update_table(table, ["time_partitioning"])
        except Exception as e:
            print('Error setting expiration to partitions of table: ', e)

    def _update_table_from_dataframe_partititon(self,df,table_id, partition_column_name, partition_column_type):
        field_partition = partition_column_name
        type_partition = partition_column_type
        # field_partition = time_partitioning['field']
        # type_partition = time_partitioning['type']
        table = table_id

        date_overwrite = df[field_partition].astype(str).unique()
        date_overwrite = tuple(date_overwrite)

        if len(date_overwrite) == 1:
            date_overwrite = "('" + date_overwrite[0] + "')"
        else:
            date_overwrite = date_overwrite

        print(date_overwrite)

        query = """DELETE FROM `{table_name}` WHERE {field_partition} in {date_overwrite}""".format(
            table_name=table_id, field_partition=field_partition, date_overwrite=date_overwrite)
        print(query)
        self.run_biqquery_to_df(query=query)

        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND',
                                            time_partitioning=bigquery.TimePartitioning(
                                                type_=type_partition,
                                                field=field_partition,  # field to use for partitioning
                                            ),
                                            autodetect=True)
        self._get_bqr_client()
        client = self.bqr_client
        job = client.load_table_from_dataframe(df, table, job_config=job_config)

        job.result()
        table = client.get_table(table)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        return table


    def update_table_from_csv(self,file_path,table_id):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
        )
        self._get_bqr_client()
        client = self.bqr_client
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        return table

    def load_table_from_gcs(self, table_id, gcs_uri, file_format:Literal['PARQUET'], write_disposition='WRITE_TRUNCATE'):
        self._get_bqr_client()
        client = self.bqr_client
        if file_format=='PARQUET':
            source_format=bigquery.SourceFormat.PARQUET
        else:
            print('Incorrect File Format')
            return False
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=source_format,
            write_disposition=write_disposition
        )
        job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job.result()
        print(f"✅ Loaded {gcs_uri} into staging table.")


    def drop_bigquery_table(self,table_id):
        self._get_bqr_client()
        client = self.bqr_client
        try:
            query_job = client.delete_table(table_id)
            query_job.result()
        except Exception as e:
            print('Error dropping table: ',e)
    
    def _get_table_schema(self, table_id):
        self._get_bqr_client()
        client = self.bqr_client
        table_schema = None
        try:
            table = client.get_table(table_id)
        except Exception as e:
            print('Error fetching table: ',e)
            return
        table_schema = list(table.schema)
        return table_schema

    def add_column_to_table(self, table_id, **kwargs):
        self._get_bqr_client()
        client = self.bqr_client
        table = client.get_table(table_id)
        new_schema = self._get_table_schema(table_id=table_id)
        if new_schema:
            pass
        else:
            return
        # new_column_list = [[i, kwargs[i]] for i in kwargs]
        for i in kwargs:
            new_schema.append(bigquery.SchemaField(i, kwargs[i]))

        table.schema = new_schema
        try:
            table = client.update_table(table, ["schema"])
            print('Update table schema successful!')
        except Exception as e:
            print('Error update table schema: ', e)