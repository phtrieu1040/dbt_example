import os
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import bigquery_datatransfer
from googleapiclient.discovery import build
import gspread
import os

# client_token = 'client_token.pickle'
# pydrive_token = 'pydrive_token.pickle'
# client_secret_file = 'client_secret.json'

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/cloud-platform"
]

class Tokenization:
    @staticmethod
    def load_cred(name, client_secret_directory):
        creds_directory = os.path.join(client_secret_directory, name)
        try:
            creds = pickle.load(open(creds_directory, 'rb'))
        except:
            return None
        return creds
    
    @staticmethod
    def create_cred(type, client_secret_directory, client_secret_file, client_token, pydrive_token):
        client_secret_file_path = os.path.join(client_secret_directory, client_secret_file)
        token_file_path = os.path.join(client_secret_directory, client_token)
        pydrive_token_file_path = os.path.join(client_secret_directory, pydrive_token)
        if type == 'client':
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file_path, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(token_file_path, 'wb') as token:
                pickle.dump(creds, token)
        elif type == 'pydrive':
            gauth = GoogleAuth()
            gauth.DEFAULT_SETTINGS['client_config_file'] = client_secret_file_path
            gauth.LocalWebserverAuth()
            with open(pydrive_token_file_path, 'wb') as token:
                pickle.dump(gauth, token)

class Authorization:
    def __init__(self, client_secret_directory, use_service_account=True):
        self._client = None
        self._service = None
        self._drive_service = None
        self._gauth = None
        self._drive = None
        self._gspread_client = None
        self._cloud_manager = None
        self._gcs_client = None
        self._gg_vpc = None
        self.scopes = SCOPES
        self.client_secret_directory = client_secret_directory
        self.client_token = 'client_token.pickle'
        self.pydrive_token = 'pydrive_token.pickle'
        self.client_secret_file = 'client_secret.json'

        if use_service_account:
            self._initialize_service_account(client_secret_directory)
        else:
            self._initialize_oauth(client_secret_directory)

    def _initialize_oauth(self, client_secret_directory):
        client_token_file_path = os.path.join(client_secret_directory, self.client_token)
        pydrive_token_file_path = os.path.join(client_secret_directory, self.pydrive_token)
        checker_client = True
        checker_pydrive = True

        while checker_client:
            creds = Tokenization.load_cred(self.client_token, client_secret_directory)
            if creds is not None and not creds.expired:
                checker_client = False
                continue
            elif (creds is not None and creds.expired) or creds is None:
                try:
                    os.remove(client_token_file_path)
                except Exception:
                    print('No Token, Now Create New Cred!')
                Tokenization.create_cred(type='client', client_secret_directory=client_secret_directory, client_token=self.client_token, pydrive_token=self.pydrive_token, client_secret_file=self.client_secret_file)
            else:
                break

        while checker_pydrive:
            gauth = Tokenization.load_cred(self.pydrive_token, client_secret_directory)
            if gauth is not None and not gauth.access_token_expired:
                checker_pydrive = False
                continue
            elif (gauth is not None and gauth.access_token_expired) or gauth is None:
                try:
                    os.remove(pydrive_token_file_path)
                except Exception:
                    print('No Drive Token, Now Create New Cred!')
                Tokenization.create_cred(type='client', client_secret_directory=client_secret_directory, client_token=self.client_token, pydrive_token=self.pydrive_token, client_secret_file=self.client_secret_file)
            else:
                break
        
        self._client = bigquery.Client(credentials=creds)
        self._service = build('sheets', 'v4', credentials=creds)
        self._drive_service = build('drive', 'v3', credentials=creds)
        self._gauth = gauth
        self._drive = GoogleDrive(gauth)
        self._gspread_client = gspread.authorize(creds)

    def _initialize_service_account(self, client_secret_directory):
        client_secret_file_path = os.path.join(client_secret_directory, self.client_secret_file)
        credentials = ServiceAccountCredentials.from_service_account_file(
            client_secret_file_path, scopes=SCOPES
        )
        self._client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        self._service = build('sheets', 'v4', credentials=credentials)
        self._drive_service = build('drive', 'v3', credentials=credentials)
        self._gspread_client = gspread.authorize(credentials)
        self._cloud_manager = build('cloudresourcemanager', 'v1', credentials=credentials)
        self._gcs_client = storage.Client.from_service_account_json(client_secret_file_path)
        self._gg_vpc = build("vpcaccess", "v1", credentials=credentials)

    @property
    def client(self):
        return self._client

    @property
    def service(self):
        return self._service
    
    @property
    def drive_service(self):
        return self._drive_service
    
    @property
    def gauth(self):
        return self._gauth
    
    @property
    def drive(self):
        return self._drive
    
    @property
    def gspread_client(self):
        return self._gspread_client
    
    @property
    def cloud_manager(self):
        return self._cloud_manager
    
    @property
    def gcs_client(self):
        return self._gcs_client
    
    @property
    def gg_vpc(self):
        return self._gg_vpc
    
class GoogleAuthManager:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self._credentials = Authorization(client_secret_directory)
        self.client_secret_directory = client_secret_directory

    @property
    def credentials(self):
        return self._credentials
    
    @credentials.setter
    def credentials(self, new_credentials):
        self._credentials = new_credentials

    def check_cred(self):
        client_token = self.credentials.client_token
        pydrive_token = self.credentials.pydrive_token
        if self.use_service_account:
            # For service account, no token expiration checks are required
            return
        else:
            creds = Tokenization.load_cred(client_token, self.client_secret_directory) 
            gauth = Tokenization.load_cred(pydrive_token, self.client_secret_directory)
            if self.credentials.client._credentials.expired or self.credentials.gauth.access_token_expired or creds is None or gauth is None:
                self.credentials = Authorization(self.client_secret_directory)

    def _get_sheet(self, url):
        self.check_cred()
        sheet = self.credentials.gspread_client.open_by_url(url=url)
        return sheet