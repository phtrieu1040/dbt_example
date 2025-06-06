from google.cloud import storage
from google.cloud import bigquery
from python_library.prefect_lib import Prefect
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import pandas as pd
import polars as pl


SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]


class GoogleCloud:
    def __init__(self):
        self.prefect = Prefect()
        self._bqr_client = None
        self._sheet_service = None
        self._drive_service = None
        self._gspread_client = None
        self._cloud_manager = None
        self._gcs_client = None
        self.creds = None
        self.keyfile_dict = None
        self.project_id = None

    def _get_creds(self):
        self.prefect.set_bqr_env_vars()
        creds = self.prefect.gcs_creds
        keyfile_dict = creds["keyfile_json"] if "keyfile_json" in creds else creds
        credentials = Credentials.from_service_account_info(keyfile_dict)
        self.creds = credentials
        self.keyfile_dict = keyfile_dict
        self.project_id = keyfile_dict["project_id"]
    
    def init_service_account(self):
        self._get_creds()
        bqr_client = bigquery.Client(credentials=self.creds, project=self.project_id)
        sheet_service = build('sheets', 'v4', credentials=self.creds)
        drive_service = build('drive', 'v3', credentials=self.creds)
        gspread_client = gspread.authorize(self.creds)
        cloud_manager = build('cloudresourcemanager', 'v1', credentials=self.creds)
        gcs_client = storage.Client.from_service_account_info(self.keyfile_dict)  # Use dict here!



        self._bqr_client = bqr_client
        self._sheet_service = sheet_service
        self._drive_service = drive_service
        self._gspread_client = gspread_client
        self._cloud_manager = cloud_manager
        self._gcs_client = gcs_client

    @property
    def bqr_client(self):
        return self._bqr_client
    
    @property
    def sheet_service(self):
        return self._sheet_service
    
    @property
    def drive_service(self):
        return self._drive_service
    
    @property
    def gspread_client(self):
        return self._gspread_client
    
    @property
    def cloud_manager(self):
        return self._cloud_manager
    
    @property
    def gcs_client(self):
        return self._gcs_client
    

class BigQuery:
    def __init__(self):
        self.google_cloud = GoogleCloud()
        self.google_cloud.init_service_account()
        self.bqr_client = self.google_cloud.bqr_client

    def run_query(self, query):
        try:
            job=self.bqr_client.query(query)
            result=job.result()
            return result
        except Exception as e:
            print(f"Error running query: {e}")
            return None
        
    def run_query_from_file(self, file_path):
        try:
            with open(file_path, "r") as file:
                query = file.read()
        except Exception as e:
            print(f"Error reading query file: {e}")
            return None
        try:
            result = self.run_query(query)
        except Exception as e:
            print(f"Error running query: {e}")
            return None
        return result
    
    def query_to_df(self, file_path=None, query=None):
        if query is None and file_path is not None:
            try:
                result=self.run_query_from_file(file_path)
            except Exception as e:
                print(f"Error running query: {e}")
                return None
        elif query is None and file_path is None:
            print("❌ No query or file path provided")
            return None
        else:
            try:
                result=self.run_query(query)
            except Exception as e:
                print(f"Error running query: {e}")
                return None
        
        if result is None:
            return None
        else:
            df=result.to_dataframe()
            return df
        
    
    
    

    


    