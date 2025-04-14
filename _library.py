from __future__ import print_function
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import credentials
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import bigquery_datatransfer
import pandas as pd
import os
from googleapiclient.discovery import build
from httplib2 import Http
from googleapiclient.http import MediaFileUpload
import base64
import time
import pickle
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread
from xlsxwriter.utility import xl_cell_to_rowcol, xl_rowcol_to_cell
import csv
from typing import Literal
from datetime import datetime, date, timedelta, timezone
import pytz
import imaplib
import email
from email.header import decode_header
import html2text
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime as lib_dt
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import atexit
import threading
import signal
import logging
from selenium.webdriver.chrome.options import Options
import getpass
import json

SCOPES=[
    # "https://www.googleapis.com/auth/bigquery",
    # "https://www.googleapis.com/auth/drive",
    # "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/cloud-platform"
    ]

client_token='token.pickle'
pydrive_token='pydrive_token.pickle'
# client_secret_file='client_secret.json'
client_secret_file='tevi_data.json'


# class Tokenization:
#     @staticmethod
#     def load_cred(name, client_secret_directory):
#         creds_directory = os.path.join(client_secret_directory,name)
#         try:
#             creds = pickle.load(open(creds_directory, 'rb'))
#         except:
#             return None
#         return creds

#     @staticmethod
#     def create_cred(type, client_secret_directory):
#         client_secret_file_path = os.path.join(client_secret_directory, client_secret_file)
#         token_file_path = os.path.join(client_secret_directory, client_token)
#         pydrive_token_file_path = os.path.join(client_secret_directory, pydrive_token)
#         if type == 'client':
#             flow = InstalledAppFlow.from_client_secrets_file(client_secret_file_path, SCOPES)
#             creds = flow.run_local_server(port=0)
#             with open(token_file_path, 'wb') as token:
#                 pickle.dump(creds, token)

#         elif type == 'pydrive':
#             gauth = GoogleAuth()
#             gauth.DEFAULT_SETTINGS['client_config_file'] = client_secret_file_path
#             gauth.LocalWebserverAuth()
#             with open(pydrive_token_file_path, 'wb') as token:
#                 pickle.dump(gauth, token)

# class Authorization:
#     def __init__(self, client_secret_directory):
#         client_token_file_path = os.path.join(client_secret_directory, client_token)
#         pydrive_token_file_path = os.path.join(client_secret_directory, pydrive_token)
#         checker_client = True
#         checker_pydrive = True
#         while checker_client:
#             creds = Tokenization.load_cred(client_token, client_secret_directory)
#             if creds is not None and not creds.expired:
#                 checker_client = False
#                 continue
#             elif (creds is not None and creds.expired) or creds is None:
#                 try:
#                     os.remove(client_token_file_path)
#                 except Exception:
#                     print('No Token, Now Create New Cred!')
#                 Tokenization.create_cred(type='client', client_secret_directory=client_secret_directory)
#             else: break

#         while checker_pydrive:
#             gauth = Tokenization.load_cred(pydrive_token, client_secret_directory)
#             if gauth is not None and not gauth.access_token_expired:
#                 checker_pydrive = False
#                 continue
#             elif (gauth is not None and gauth.access_token_expired) or gauth is None:
#                 try:
#                     os.remove(pydrive_token_file_path)
#                 except Exception:
#                     print('No Drive Token, Now Create New Cred!')
#                 Tokenization.create_cred(type='pydrive', client_secret_directory=client_secret_directory)
#             else: break
        
#         self._client = bigquery.Client(project='tiki-analytics-dwh', credentials=creds)
#         self._service = build('sheets', 'v4', credentials=creds)
#         self._drive_service = build('drive', 'v3', credentials=creds)
#         self._gauth = gauth
#         self._drive = GoogleDrive(gauth)
#         self._gspread_client = gspread.authorize(creds)
    
#     @property
#     def client(self):
#         return self._client
    
#     @property
#     def service(self):
#         return self._service
    
#     @property
#     def drive_service(self):
#         return self._drive_service
    
#     @property
#     def gauth(self):
#         return self._gauth

#     @property
#     def drive(self):
#         return self._drive
    
#     @property
#     def gspread_client(self):
#         return self._gspread_client


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
    def create_cred(type, client_secret_directory):
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
        
        if use_service_account:
            self._initialize_service_account(client_secret_directory)
        else:
            self._initialize_oauth(client_secret_directory)

    def _initialize_oauth(self, client_secret_directory):
        client_token_file_path = os.path.join(client_secret_directory, client_token)
        pydrive_token_file_path = os.path.join(client_secret_directory, pydrive_token)
        checker_client = True
        checker_pydrive = True

        while checker_client:
            creds = Tokenization.load_cred(client_token, client_secret_directory)
            if creds is not None and not creds.expired:
                checker_client = False
                continue
            elif (creds is not None and creds.expired) or creds is None:
                try:
                    os.remove(client_token_file_path)
                except Exception:
                    print('No Token, Now Create New Cred!')
                Tokenization.create_cred(type='client', client_secret_directory=client_secret_directory)
            else:
                break

        while checker_pydrive:
            gauth = Tokenization.load_cred(pydrive_token, client_secret_directory)
            if gauth is not None and not gauth.access_token_expired:
                checker_pydrive = False
                continue
            elif (gauth is not None and gauth.access_token_expired) or gauth is None:
                try:
                    os.remove(pydrive_token_file_path)
                except Exception:
                    print('No Drive Token, Now Create New Cred!')
                Tokenization.create_cred(type='pydrive', client_secret_directory=client_secret_directory)
            else:
                break
        
        self._client = bigquery.Client(credentials=creds)
        self._service = build('sheets', 'v4', credentials=creds)
        self._drive_service = build('drive', 'v3', credentials=creds)
        self._gauth = gauth
        self._drive = GoogleDrive(gauth)
        self._gspread_client = gspread.authorize(creds)

    def _initialize_service_account(self, client_secret_directory):
        client_secret_file_path = os.path.join(client_secret_directory, client_secret_file)
        credentials = ServiceAccountCredentials.from_service_account_file(
            client_secret_file_path, scopes=SCOPES
        )
        self._client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        self._service = build('sheets', 'v4', credentials=credentials)
        self._drive_service = build('drive', 'v3', credentials=credentials)
        self._gspread_client = gspread.authorize(credentials)
        self._cloud_manager = build('cloudresourcemanager', 'v1', credentials=credentials)
        self._gcs_client = storage.Client.from_service_account_json(client_secret_file_path)

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

    
class GoogleFile:
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
    
    def get_permissions_email_from_google_sheet(self, url):
        sheet = self._get_sheet(url=url)
        permissions = sheet.list_permissions()
        df_permissions = pd.DataFrame(permissions)
        emails = df_permissions.emailAddress.tolist()
        return emails
    
    def get_full_permissions_from_google_sheet_df(self, url):
        sheet = self._get_sheet(url=url)
        permissions = sheet.list_permissions()
        df_permissions = pd.DataFrame(permissions)
        return df_permissions
    
    def get_full_permissions_from_google_sheet_raw(self, url):
        sheet = self._get_sheet(url=url)
        permissions = sheet.list_permissions()
        return permissions
    
    def transfer_ownership(self, email, file_id, notify=True):
        client = self.credentials.gspread_client
        try:
            client.insert_permission(file_id=file_id,
                                     value=email,
                                     perm_type='user',
                                     role='owner',
                                     notify=notify,
                                     )
        except Exception as e:
            print(e + " for file {}".format(file_id))
    
    def delete_google_sheet_file(self, file_id):
        self.check_cred()
        self.credentials.gspread_client.del_spreadsheet(file_id=file_id)

    def get_ggs_ids(self):
        a = list(self.credentials.gspread_client.list_spreadsheet_files())
        return a
    
    def create_new_google_sheet(self, sheet_name, email_to_share, perm_type = 'user', role = 'writer'):
        sheet = self.credentials.gspread_client.create(sheet_name)
        if type(email_to_share) is list:
            for email in email_to_share:
                sheet.share(email, perm_type = perm_type, role = role)
        else:
            sheet.share(email_to_share, perm_type = perm_type, role = role)
        sheet_url = sheet.url
        print('new ggs file has been created, url: ', sheet_url)
        return sheet_url

    def _delete_sheet(self,sheet_name, url):
        sheet = self._get_sheet(url=url)
        if type(sheet_name) is list:
            for sheet_name in sheet_name:
                try:
                    worksheet = sheet.worksheet(sheet_name)
                    sheet.del_worksheet(worksheet)
                except Exception as e:
                    print(f"Error deleting worksheet '{sheet_name}': {e}")
        else:
            try:
                worksheet = sheet.worksheet(sheet_name)
                sheet.del_worksheet(worksheet)
            except Exception as e:
                print(f"Error deleting worksheet '{sheet_name}': {e}")
    
    def duplicate_google_sheet(self, source_id, target_file_name, email_to_share=None, copy_permissions=True, source_sheet_to_duplicate=None):
        self.check_cred()
        sheet = self.credentials.gspread_client.copy(source_id, title=target_file_name, copy_permissions=copy_permissions)
        sheet_url = sheet.url
        print(sheet_url)
        if email_to_share is not None:
            self.grant_ggs_permission(url=sheet_url, email=email_to_share, role='writer', notify=True)
        worksheets = sheet.worksheets()
        worksheet_names = [worksheet.title for worksheet in worksheets]
        if source_sheet_to_duplicate is None:
            return sheet_url
        else:
            unwanted_sheets = MyFunction.non_outer_join_a_vs_b(a=source_sheet_to_duplicate, b=worksheet_names)
            self._delete_sheet(sheet_name=unwanted_sheets, url=sheet_url)                               
            return sheet_url
    
    def _remove_google_sheet_permission(self, sheet, email):
        sheet = sheet
        try:
            sheet.remove_permissions(email)
        except Exception as e:
            print(f"Error deleting permission '{email}': {e}")

    def remove_ggs_permissions(self, url, email):
        sheet = self._get_sheet(url)
        if type(email) is list:
            for email in email:
                self._remove_google_sheet_permission(sheet=sheet, email=email)
        else:
            self._remove_google_sheet_permission(sheet=sheet, email=email)

    def _grant_google_sheet_permission(self, sheet, email, role, notify):
        sheet = sheet
        try:
            sheet.share(email, perm_type = 'user', role = role, notify=notify)
        except Exception as e:
            print(f"Error granting permission to '{email}': {e}")

    def grant_ggs_permission(self, url, email, role, notify):
        sheet = self._get_sheet(url)
        if type(email) is list:
            for email in email:
                self._grant_google_sheet_permission(sheet=sheet, email=email, role=role, notify=notify)
        else:
            self._grant_google_sheet_permission(sheet=sheet, email=email, role=role, notify=notify)

    def _get_file_worksheet_list(self, url):
        sheet = self._get_sheet(url)
        worksheets = sheet.worksheets()
        worksheet_names = [worksheet.title for worksheet in worksheets]
        return worksheet_names

    def is_worksheet_completely_empty(self, url, worksheet_name):
        sh = self._get_sheet(url)
        wks = sh.worksheet(worksheet_name)
        # Fetch all values
        all_values = wks.get_all_values()
        # Check if all values are empty
        if not all_values or all([not cell for row in all_values for cell in row]):
            return True
        else:
            return False
        
    def _batch_transfer_ownership_callback(self, request_id, response, exception):
        if exception is not None:
            # If there's an error, add to the failed_transfer list
            print(f"An error occurred for {request_id}: {exception}")
            self.un_successfully_transferred.append(request_id)
        else:
            # If successful, add to the successfully_transferred list
            print(f"Successfully transferred ownership for {request_id}")
            self.successfully_transferred.append(request_id)

    def batch_transfer_ownership(self, file_id_list, email_to_transfer, role='owner', type='user'):
        self.successfully_transferred = []
        self.un_successfully_transferred = []
        batch = self.credentials.drive_service.new_batch_http_request(callback=self._batch_transfer_ownership_callback)
        for file_id in file_id_list:
            new_permissions = {
                'type': type,
                'role': role,
                'emailAddress': email_to_transfer,
            }
            request_id = file_id  # Using file_id as the request_id for easy tracking
            batch.add(
                self.credentials.drive_service.permissions().create(
                    fileId=file_id,
                    body=new_permissions,
                    transferOwnership=(role == 'owner'),
                    fields='id, role',
                ),
                request_id=request_id
            )
        batch.execute()

        # Return or process the lists of transferred and failed IDs as needed
        return self.successfully_transferred, self.un_successfully_transferred

    # def read_google_sheet_by_url(self, url, worksheet_name='', header_row=1):
    #     sh = self._get_sheet(url)
    #     if worksheet_name:
    #         wks = sh.worksheet(worksheet_name)
    #     else:
    #         wks = sh.worksheet(0)
    #     values = wks.get_all_values()
    #     data = pd.DataFrame(values[header_row:], columns=values[header_row-1])
    #     return data
    
    def upload_file_to_drive(self, local_file_path, remote_file_name=None, file_id=None, file_url=None, mime_type=None):
        self.check_cred()

        if file_url and not file_id:
            try:
                file_id = MyFunction.extract_google_drive_id(file_url)
            except Exception as e:
                print('Could not extract file Id from URL: ', {file_url}, ". Error: ", e)
        
        if remote_file_name is None:
            remote_file_name = os.path.basename(local_file_path)

        if file_id is None and remote_file_name is not None:
            query = f'name = "{remote_file_name}" and trashed = false'
            results = self.credentials.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            items = results.get('files', [])

            if items:
                file_id = items[0]['id']
                print(f'Found file "{remote_file_name}", will update trucate it with ID: "{file_id}"')

        try:
            if file_id:
                media = MediaFileUpload(local_file_path, mimetype=mime_type, resumable=True)
                updated_file = self.credentials.drive_service.files().update(
                    fileId=file_id,
                    media_body=media
                ).execute()
                print(f'File updated successfully. File ID: "{updated_file['id']}"')
                return updated_file['id']
            
            else:
                file_metadata = {
                    'name': remote_file_name
                }
                media = MediaFileUpload(local_file_path, mimetype=mime_type, resumable=True)
                created_file = self.credentials.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f'File uploaded successfully. File ID: "{created_file['id']}"')
                return created_file['id']
        except Exception as e:
            print(f'Error uploading file to Google Drive: "{e}"')
            return None

    def read_google_sheet_by_url(self, url, worksheet_name='', header_row=1, range_name=''):
        sh = self._get_sheet(url)
        if worksheet_name:
            wks = sh.worksheet(worksheet_name)
        else:
            wks = sh.get_worksheet(0)

        if range_name:
            values = wks.get(range_name)
            if header_row > 1:
                values = values[header_row-1:]
            data = pd.DataFrame(values[1:], columns=values[0])
        else:
            values = wks.get_all_values()
            data = pd.DataFrame(values[header_row:], columns=values[header_row-1])

        return data
    
    def write_google_sheet_by_url(
            self, df_to_write,
            url = 'https://docs.google.com/spreadsheets/d/1DuI6q1QuqhA9qmR5fULaQC8j_68E30Us13hDPBotXuc/edit?gid=1458495087#gid=1458495087',
            worksheet_name = 'python_destination',
            start_cell='A1',
            delete_before_write=True,
            copy_head=True):
        sh = self._get_sheet(url)
        wks = sh.worksheet(worksheet_name)
        if delete_before_write:
            row, col = xl_cell_to_rowcol(start_cell)
            end_col = xl_rowcol_to_cell(0, col + len(df_to_write.columns) - 1)
            end_col = end_col.rstrip("1")
            max_row = len(wks.get_all_records()) + 1
            if max_row > 0:
                end_row = str(max_row)
                end_cell = end_col + end_row
            else:
                end_cell = end_col + "1"
            range_del = worksheet_name + "!" + start_cell + ":" + end_cell
            try:
                sh.values_clear(range_del)
            except:
                self._clear_sheet_by_client(sheetID=MyFunction.extract_sheet_id(url), sheet_name=worksheet_name)
        df_to_write = df_to_write.astype('string').fillna('')
        wks.update(start_cell, [df_to_write.columns.values.tolist()] + df_to_write.values.tolist()
                   , value_input_option='USER_ENTERED')

    def _get_drive_file(self, file_id):
        self.check_cred()
        file = self.credentials.drive.CreateFile({'id': file_id})
        return file

    def read_google_drive_xlsx(self, file_id, sheet_name):
        file = self._get_drive_file(file_id=file_id)
        file.GetContentFile('temp.xlsx')
        df = pd.read_excel('temp.xlsx', sheet_name=sheet_name, dtype = 'str')
        os.remove('temp.xlsx')
        return df

    def read_google_drive_xlsx_verify_by_sheet_name_list(self, file_id, verify_list):
        file = self._get_drive_file(file_id=file_id)
        file.GetContentFile('temp.xlsx')
        sheet_names = pd.ExcelFile('temp.xlsx', engine='openpyxl').sheet_names
        sheet_names_idx = 0
        verify_list_idx = 0
        sheet_names_len = len(sheet_names)
        verify_list_len = len(verify_list)
        while sheet_names_idx < sheet_names_len and verify_list_idx < verify_list_len:
            if sheet_names[sheet_names_idx] == verify_list[verify_list_idx]:
                sheet_name = sheet_names[sheet_names_idx]
                break
            elif sheet_names_idx == sheet_names_len - 1:
                sheet_names_idx = 0
                verify_list_idx += 1
                continue
            else:
                sheet_names_idx += 1
        df = pd.read_excel('temp.xlsx', sheet_name=sheet_name, dtype = 'str')
        os.remove('temp.xlsx')
        return df

    def _get_sheet_by_service(self):
        self.check_cred()
        service_sheet = self.credentials.service.spreadsheets()
        return service_sheet
    
    def _modify_permision_by_drive_service(self, email, sheet_id, role:Literal['reader', 'writer', 'owner'], type: Literal['anyone', 'user'], transfer_ownership=False):
        self.check_cred()
        new_permissions = {
            'role': role,
            'type': type,
            'emailAddress': email,
        }
        try:
            self.credentials.drive_service.permissions().create(fileId=sheet_id,
                                                               body=new_permissions,
                                                               transferOwnership=transfer_ownership).execute()
        except Exception as e:
            print(e)

    def _get_files_and_email_of_owner_all_page(self, all_page=True):
        mime_types = [
            "application/vnd.google-apps.spreadsheet",
            "application/vnd.google-apps.document",
            "application/vnd.google-apps.presentation",
            # "application/vnd.google-apps.shortcut"
        ]
        file_extensions = [
            '.xlsx',
            '.drawio',
            '.docx',
            '.pptx',
            '.pdf',
            'ipynb'
        ]
        query_mime_types = "(" + " or ".join([f"mimeType='{mime_type}'" for mime_type in mime_types]) + ")"
        query_extension = "(" + " or ".join([f"name contains '{extension}'" for extension in file_extensions]) + ")"
        query = f"({query_mime_types} and 'me' in owners and trashed=false) or ({query_extension} and 'me' in owners and trashed=false)"
        # query = f"trashed=true"
        service = self.credentials.drive_service
        nextPageToken = None
        full_file_info = {}
        original_owner = None
        pattern = 'results-\d+-\d+|Untitled spreadsheet|bq-results-\d+-\d+'

        try:
            while True:
                response = service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, owners(emailAddress))',
                    pageToken=nextPageToken
                ).execute()

                files = response.get('files', [])

                if all_page:
                    nextPageToken = response.get('nextPageToken', None)
                else:
                    nextPageToken = None

                for file in files:
                    name = file.get('name')
                    if re.match(pattern=pattern, string=name):
                        # Skip files that match the pattern
                        continue

                    file_id = file.get('id')
                    if not original_owner and file.get('owners'):  # Ensure there are owners to avoid key errors
                        original_owner = file['owners'][0].get('emailAddress', None)
                    if name and file_id:  # Add file info if both name and ID are available
                        full_file_info[name] = file_id

                if not nextPageToken:
                    break
        except Exception as e:
            print('Error getting files_data: ', e)
            return {}, None

        return full_file_info, original_owner

    def _add_new_worksheet(self, url, new_sheet_name):
        sh = self._get_sheet(url)
        worksheet = sh.add_worksheet(title=new_sheet_name, rows=100, cols=10)
        return worksheet
    
    def _clear_sheet_by_client(self, sheetID, sheet_name):
        sheet = self._get_sheet_by_service()
        sheet.values().clear(spreadsheetId=sheetID, range=sheet_name).execute()

    def read_google_sheet(self, sheetID, sheet_name, header_row = 1):
        sheet = self._get_sheet_by_service()
        result = sheet.values().get(spreadsheetId=sheetID, range=sheet_name).execute()
        values = result.get('values', [])
        if not values:
            print('No data found.')
        else:
            values_with_none = [[None if cell == '' else cell for cell in row] for row in values]
            df = pd.DataFrame(values_with_none)
        df.columns = df.iloc[header_row-1]
        df = df[header_row:]
        return df

    def write_google_sheet(self,sheetID,sheet_name,df_to_write):
        sheet = self._get_sheet_by_service()
        self._clear_sheet_by_client(sheetID=sheetID, sheet_name=sheet_name)
        values = [df_to_write.columns.values.tolist()]
        values.extend(df_to_write.values.tolist())
        data = [{'range': sheet_name, 'values': values}]
        batch_update_values_request_body = {
            'value_input_option': 'USER_ENTERED',
            'data': data}
        result = (
            sheet
            .values()
            .batchUpdate(spreadsheetId=sheetID,body=batch_update_values_request_body)
            .execute())
        return result
    
    def read_drive_file(self, file_url, output_file: Literal['temp.csv', 'temp.xlsx'], worksheet_name=None, header_row=0):
        from googleapiclient.http import MediaIoBaseDownload
        file_id = MyFunction.extract_google_drive_id(file_url)
        request = self.credentials.drive_service.files().get_media(fileId=file_id)
        df = pd.DataFrame()
        with open(output_file, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f'Download {int(status.progress() * 100)}%.')
        try:
            if output_file == 'temp.csv':
                df = pd.read_csv(output_file, dtype='str')
            elif output_file == 'temp.xlsx':
                if worksheet_name:
                    df = pd.read_excel(output_file, dtype='str', sheet_name=worksheet_name, header=header_row)
                else:
                    df = pd.read_excel(output_file, dtype='str', header=header_row)
            else:
                print('Incorrect output_file, stop process')
                os.remove(output_file)
        except Exception as e:
            os.remove(output_file)
            print(e)
        return df
    
    @staticmethod
    def read_drive_csv_other(file_id):
        import gdown
        url = f"https://drive.google.com/uc?id={file_id}"
        output = 'temp.csv'
        gdown.download(url, output, quiet=False)
        df = pd.read_csv(output, dtype='str')
        os.remove(output)
        return df


class GoogleCloudPlatform:
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
        if self.use_service_account:
            # For service account, no token expiration checks are required
            return
        else:
            creds = Tokenization.load_cred(client_token, self.client_secret_directory) 
            gauth = Tokenization.load_cred(pydrive_token, self.client_secret_directory)
            if self.credentials.client._credentials.expired or self.credentials.gauth.access_token_expired or creds is None or gauth is None:
                self.credentials = Authorization(self.client_secret_directory)

    def get_cloud_manager(self):
        self.check_cred()
        cloud_manager = self.credentials.cloud_manager
        return cloud_manager
    
    def get_iam_policy(self, project_id):
        self.check_cred()
        cloud_manager = self.get_cloud_manager()
        policy = cloud_manager.projects().getIamPolicy(
            resource=project_id,
            body={}
        ).execute()
        return policy


class GoogleCloudStorage:
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
        if self.use_service_account:
            # For service account, no token expiration checks are required
            return
        else:
            creds = Tokenization.load_cred(client_token, self.client_secret_directory) 
            gauth = Tokenization.load_cred(pydrive_token, self.client_secret_directory)
            if self.credentials.client._credentials.expired or self.credentials.gauth.access_token_expired or creds is None or gauth is None:
                self.credentials = Authorization(self.client_secret_directory)

    def get_gcs_client(self):
        self.check_cred()
        gcs_client = self.credentials.gcs_client
        return gcs_client
    

    def upload_file_to_gcs(self, source_file_path, bucket_folder_path):
        parts = bucket_folder_path.strip('/').split('/', 1)
        bucket_name = parts[0]
        gcs_client = self.get_gcs_client()

        filename = os.path.basename(source_file_path)
        
        if len(parts) > 1:
            folder_path = parts[1]
            if not folder_path.endswith('/'):
                folder_path += '/'
            destination_blob_name = f"{folder_path}{filename}"
        else:
            destination_blob_name = filename
        
        bucket = gcs_client.bucket(bucket_name)
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
        return f"gs://{bucket_name}/{destination_blob_name}"
    

    def download_file(self, bucket_name, source_blob_name:Literal['file path in GCS'], destination_file_path:Literal['file path in local']=None):
        gcs_client = self.get_gcs_client()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        # If no destination path is provided, use current working directory + original filename
        if destination_file_path is None:
            # Extract the filename from the source blob name
            filename = os.path.basename(source_blob_name)
            # Use current working directory
            destination_file_path = os.path.join(os.getcwd(), filename)
        
        # Download the file
        blob.download_to_filename(destination_file_path)
        
        print(f"File gs://{bucket_name}/{source_blob_name} downloaded to {destination_file_path}")
        return destination_file_path
    

    def upload_dataframe_to_gcs(self, dataframe, bucket_folder_path, filename, file_format='csv', **kwargs):
        """
        Upload a pandas DataFrame directly to GCS bucket.
        
        Args:
            dataframe: pandas DataFrame to upload
            bucket_folder_path: String in format "bucket_name/folder_path/"
                            (e.g., "tevi_data_team/crypto_airdrop/")
            filename: Name to give the file in the bucket (without extension)
            file_format: Format to save the dataframe as ('csv', 'parquet', 'excel', etc.)
            **kwargs: Additional arguments to pass to the pandas to_* method
        
        Returns:
            The public URL of the uploaded file
        """
        import io
        
        gcs_client = self.get_gcs_client()

        # Parse bucket and folder path
        parts = bucket_folder_path.strip('/').split('/', 1)
        bucket_name = parts[0]
        
        # Add file extension if not present
        if not filename.endswith(f'.{file_format}'):
            filename = f"{filename}.{file_format}"
        
        # Construct destination path
        if len(parts) > 1:
            folder_path = parts[1]
            # Ensure folder path ends with a slash
            if not folder_path.endswith('/'):
                folder_path += '/'
            destination_blob_name = f"{folder_path}{filename}"
        else:
            destination_blob_name = filename
        
        # Get bucket
        bucket = gcs_client.bucket(bucket_name)
        
        # Create blob
        blob = bucket.blob(destination_blob_name)
        
        # Convert dataframe to in-memory file
        buffer = io.BytesIO()
        
        # Save dataframe to buffer in the specified format
        if file_format.lower() == 'csv':
            dataframe.to_csv(buffer, index=False, **kwargs)
        elif file_format.lower() == 'parquet':
            dataframe.to_parquet(buffer, index=False, **kwargs)
        elif file_format.lower() == 'excel' or file_format.lower() == 'xlsx':
            dataframe.to_excel(buffer, index=False, **kwargs)
        elif file_format.lower() == 'json':
            dataframe.to_json(buffer, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Set buffer's position to the beginning
        buffer.seek(0)
        
        # Upload from buffer
        content_type_map = {
            'csv': 'text/csv',
            'parquet': 'application/octet-stream',
            'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'json': 'application/json'
        }
        content_type = content_type_map.get(file_format.lower(), 'application/octet-stream')
        
        blob.upload_from_file(buffer, content_type=content_type)
        
        print(f"DataFrame uploaded to gs://{bucket_name}/{destination_blob_name}")
        return f"gs://{bucket_name}/{destination_blob_name}"
    
    def list_files(self, bucket_name, prefix=None):
        """List all files in a bucket with optional prefix filter"""
        gcs_client = self.get_gcs_client()
        bucket = gcs_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]

class CrawlingWeb:
    class Selenium:
        def __init__(self):
            self.options = webdriver.ChromeOptions()
            self.options.add_argument("--disable-gpu")
            self.options.add_argument("--no-sandbox")
            self.options.add_argument("--disable-dev-shm-usage")
            self.options.add_argument("--headless")
            self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
            self.driver = None
        
        def _get_selenium_driver(self):
            # Initialize and return the Selenium WebDriver
            if self.driver is None:
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager(driver_version='131.0.6778.265').install()),
                    options=self.options
                )
            return self.driver
        
    class CrawlBDSdotVN:

        def __init__(self):
            selenium = CrawlingWeb.Selenium()
            self.driver = selenium._get_selenium_driver()

        def _get_all_property_urls(self, url_list):
            all_property_urls = []
            for url in url_list:
                
                try:
                    self.driver.get(url)
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "js__product-link-for-product-id"))
                    )
                    # Extract all property links
                    links = self.driver.find_elements(By.CLASS_NAME, "js__product-link-for-product-id")
                    hrefs = [link.get_attribute("href") for link in links]
                    for href in hrefs:
                        url_dictionary = {}
                        url_dictionary['source'] = url
                        url_dictionary['property_url'] = href
                        all_property_urls.append(url_dictionary)
                except Exception as e:
                    print(f"An error occurred while collecting links from {url}: {e}")
                
            return all_property_urls
        
        def _get_property_data_from_web(self, url_list, property_data):
            all_property_urls = self._get_all_property_urls(url_list=url_list)


            # property_data = []
            gmt_plus_7 = timezone(timedelta(hours=7))
            current_datetime = datetime.now(gmt_plus_7).strftime('%Y-%m-%d')
            for property in all_property_urls:
                property_url = property['property_url']
                if property_url == 'https://www.hlbank.com.vn/loan-leadform':
                    continue
                else: pass

                try:
                    self.driver.get(property_url)
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "re__pr-specs-content-v2"))
                    )

                    # Extract property details
                    spec_items = self.driver.find_elements(By.CLASS_NAME, "re__pr-specs-content-item")
                    property_details = {}
                    property_details["Property URL"] = property_url
                    property_details['Datetime Collected At'] = current_datetime
                    property_details['Source URL'] = property['source']
                    
                    for item in spec_items:
                        try:
                            title = item.find_element(By.CLASS_NAME, "re__pr-specs-content-item-title").text
                            value = item.find_element(By.CLASS_NAME, "re__pr-specs-content-item-value").text
                            property_details[title] = value
                        except Exception:
                            continue  # Skip if title or value is missing

                    # Extract the district from the breadcrumb
                    breadcrumb_items = self.driver.find_elements(By.CLASS_NAME, "re__link-se")
                    district = None
                    for item in breadcrumb_items:
                        if item.get_attribute("level") == "3":  # Check for district level
                            district = item.text
                            break

                    # Extract the address_text
                    address_text = None
                    try:
                        address_text_element = self.driver.find_element(By.CLASS_NAME, "js__pr-address")
                        address_text = address_text_element.text
                    except Exception:
                        pass  # Skip if address text is not available

                    # Add district and address_text to the dictionary
                    property_details["District"] = district
                    property_details["Address"] = address_text

                    # Append to the list
                    property_data.append(property_details)

                except Exception as e:
                    print(f"An error occurred while collecting details from {property_url}: {e}")
                
            # return property_data

        def __del__(self):
            if self.driver:
                print("Quitting WebDriver...")
                self.driver.quit()



class GoogleMail:
    def __init__(self, email_address, password) -> None:
        self.email_address = email_address
        self.password = password
        self._gmail = imaplib.IMAP4_SSL("imap.gmail.com")

    @property
    def gmail(self):
        return self._gmail
    
    @gmail.setter
    def gmail(self, new_gmail):
        self._gmail = new_gmail
    
    def _login_gmail(self):
        self._check_gmail_connection()
        try:
            self.gmail.login(self.email_address, self.password)
        except Exception as e:
            pass

    def _check_gmail_connection(self):
        try:
            status, _ = self.gmail.noop()
        except Exception as e:
            status = None
        if status == 'OK':
            return
        else:
            self.gmail = imaplib.IMAP4_SSL("imap.gmail.com")

    def _log_out_email(self):
        self.gmail.logout()

    def get_email_ids(self):
        self._check_gmail_connection()
        self._login_gmail()
        self._read_inbox()
        _, email_ids_str = self.gmail.search(None, 'All')
        email_ids = email_ids_str[0].split()
        return email_ids
    
    def _read_inbox(self):
        self.gmail.select('inbox')

    def fetch_email_by_id(self, id):
        self._login_gmail()
        self._read_inbox()
        _, msg_data = self.gmail.fetch(id, "(RFC822)")
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)
        subject, encoding = decode_header(msg["Subject"])[0]
        from_address = msg.get("From")
        date_sent = msg.get("Date")
        try:
            date_sent = MyFunction.convert_long_date_to_gmt7(date_sent)
        except Exception as e:
            print(id, e)
        print(date_sent)
        body=''
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))
                if "text/plain" in content_type and "attachment" not in content_disposition:
                    body += part.get_payload(decode=True).decode()
                elif "text/html" in content_type and "attachment" not in content_disposition:
                    body += part.get_payload(decode=True).decode()

                    # # Convert HTML to plain text
                    # body += html2text.html2text(part.get_payload(decode=True).decode())
                
        else:
            body = msg.get_payload(decode=True).decode()
        # replacements = {'\n': ' ', '\r': '', '“': '', '”':'', '₫':''} 
        # body = ''.join(replacements.get(c, c) for c in body)
        body = MyFunction._remove_other_symbols(input_str=body)
        body = MyFunction._remove_accents(body)
        # body = MyFunction._remove_strange_symbols([body])
        # body = body.replace('\n',' ').replace('\r','').replace('“', '')
        try:
            subject = subject.decode('utf-8')
            subject = MyFunction._remove_accents(subject)
            subject = MyFunction._remove_strange_symbols([subject])
        except:
            pass
        sender = MyFunction._extract_email(from_address)
        # print(sender, type(sender))
        return str(id), subject, sender, date_sent, body
    
    def _extract_bank_body(self, email, body):
        method_dictionary = {
            'alerts@citibank.com.vn': self.__extract_citibank_body,
            'info@myvib.vib.com.vn': self.__extract_vib_body,
            'info@vib.com.vn': self.__extract_vib_credit_body
        }
        return method_dictionary[email](body)
    
    def __extract_citibank_body(self, body):
        email_body = html2text.html2text(body)
        match = re.search(r'VND([\d,]+)', email_body)
        if match:
            result = match.group(1)
            result = result.replace(',','')
            result = int(result)
            return result
        else:
            return None
    
    def __extract_vib_credit_body(self, body):
        email_body = html2text.html2text(body)
        match = re.search(r'\nGia tri: \*\*([\d,]+) VND\*\*', email_body)
        if match:
            result = match.group(1)
            result = result.replace(',','')
            result = int(result)
            return result
        else:
            return None

    def __extract_vib_body(self, body):
        email_body = html2text.html2text(body)
        match = re.search(r'\nSo tien\s*\|\s*([\d,]+)', email_body)
        if match:
            result = match.group(1)
            result = result.replace(',','')
            result = int(result)
            return result
        else:
            match = re.search(r'\nSo tien thanh toan\s*\|\s*([\d,]+)', email_body)
            if match:
                result = match.group(1)
                result = result.replace(',','')
                result = int(result)
                return result
            else: return None

    def send_email(self, subject, body, to_email):
        msg = MIMEMultipart()
        msg['From'] = self.email_address
        msg['To'] = to_email
        msg['Subject'] = subject

        # Convert DataFrame to HTML table
        if type(body) is pd.core.frame.DataFrame:
            body_type = 'html'
            email_body = body.to_html(index=False)
        else:
            body_type = 'plain'
            email_body = body

        # Attach the HTML table to the email body
        msg.attach(MIMEText(email_body, body_type))

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_address, self.password)
            text = msg.as_string()
            server.sendmail(self.email_address, to_email, text)
            server.quit()
            print("Email sent successfully!")
        except Exception as e:
            print(f"Error sending email: {e}")

class Bigquery:
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
        if self.use_service_account:
            # For service account, no token expiration checks are required
            return
        else:
            creds = Tokenization.load_cred(client_token, self.client_secret_directory) 
            gauth = Tokenization.load_cred(pydrive_token, self.client_secret_directory)
            if self.credentials.client._credentials.expired or self.credentials.gauth.access_token_expired or creds is None or gauth is None:
                self.credentials = Authorization(self.client_secret_directory)
    
    def _get_bqr_client(self):
        self.check_cred()
        client = self.credentials.client
        return client
    
    def list_all_scheduled_queries(self, location):
        client = self._get_bqr_client()
        cred = client._credentials
        project_id = client.project
        return cred, project_id

    def drop_bigquery_table(self,table_id):
        client = self._get_bqr_client()
        try:
            query_job = client.delete_table(table_id)
            query_job.result()
        except Exception as e:
            print('Error dropping table: ',e)
    
    def _get_table_schema(self, table_id):
        client = self._get_bqr_client()
        table_schema = None
        try:
            table = client.get_table(table_id)
        except Exception as e:
            print('Error fetching table: ',e)
            return
        table_schema = list(table.schema)
        return table_schema

    def add_column_to_table(self, table_id, **kwargs):
        client = self._get_bqr_client()
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


    def bigquery_operation(self, query):
        client = self._get_bqr_client()
        try:
            query_job = client.query(query)
            query_job.result()
        except Exception as e:
            print('Error running query: ',e)

    def run_biqquery_to_df(self,query):
        query_string = query
        client = self._get_bqr_client()
        try:
            query_job = client.query(query_string)
            query_job.result()
            df = query_job.to_dataframe()
            return df
        except Exception as e:
            print('Error while fetching data to df: ', e)
    
    def create_ingestion_time_partitioned_table(self, table_id):
        # once the table is created, write append to it, 
        from google.cloud.bigquery import Table, TimePartitioning, TimePartitioningType
        client = self._get_bqr_client()
        table = Table(table_id)
        table.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY) # must only be day for ingestion time table, can't be any other.
        table.require_partition_filter = True
        table = client.create_table(table)

    def _enable_partition_filter(self, table_id):
        client = self._get_bqr_client()
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
            table = self._get_bqr_client().get_table(table_id)
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
        table = self._get_bqr_client().get_table(table)
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
        job = self._get_bqr_client().load_table_from_dataframe(df,table,job_config=job_config)

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
        self._get_bqr_client().load_table_from_dataframe(dataframe=df,
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
        client = self._get_bqr_client()
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
        Bigquery.run_biqquery_to_df(query=query)

        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND',
                                            time_partitioning=bigquery.TimePartitioning(
                                                type_=type_partition,
                                                field=field_partition,  # field to use for partitioning
                                            ),
                                            autodetect=True)
        job = self._get_bqr_client().load_table_from_dataframe(df, table, job_config=job_config)

        job.result()
        table = self._get_bqr_client().get_table(table)
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
        client = self.credentials.client
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
    
    def _set_expiration_to_table(self, table_id, expiration_day):
        client = self._get_bqr_client()
        table = client.get_table(table_id)
        table.expires = lib_dt.datetime.now() + lib_dt.timedelta(days=expiration_day)
        client.update_table(table, ['expires'])

class MyLibrary:
    def __init__(self,client_secret_directory) -> None:
        # client_secret_directory = r'C:\trieu.pham\python\bigquery'
        # client_secret_directory = r'C:\Python\file_token'
        self._bigquery = Bigquery(client_secret_directory)
        self._google = GoogleFile(client_secret_directory)
        self._storage = GoogleCloudStorage(client_secret_directory)
        self._selenium = BackgroundSelenium()
        self._selenium_v2 = BackgroundSelenium_v2()
        self._improved_selenium = ImprovedBackgroundSelenium()
        self._google_platform = GoogleCloudPlatform(client_secret_directory)
    
    @property
    def bigquery(self):
        return self._bigquery
    
    @property
    def google(self):
        return self._google
    
    @property
    def storage(self):
        return self._storage
    
    @property
    def selenium(self):
        return self._selenium
    
    @property
    def selenium_v2(self):
        return self._selenium_v2
    
    @property
    def improved_selenium(self):
        return self._improved_selenium
    
    @property
    def google_platform(self):
        return self._google_platform

    @property
    def function(self):
        return MyFunction()

class MyFunction:
    class MyVisualization:
        import plotly.graph_objects as go
        @classmethod
        def waterfall_chart(cls,
                            df,
                            columns,
                            sub_total_name,
                            chart_title,
                            textposition:Literal['inside', 'outside', 'auto', 'none'],
                            font_size,
                            positive_color,
                            negative_color,
                            dash_style:Literal['solid', 'dot', 'dash', 'longdash', 'dashdot', 'longdashdot']='dash',
                            sub_total_color=None,
                            title_size=24,
                            font_style='Arial',
                            zero_line='blue',
                            xaxis_name='Parameters',
                            yaxis_name='Values',
                            dash_color='black',
                            xaxis_size=16,
                            yaxis_size=16,
                            x_tick_size=20,
                            y_tick_size=14,
                            ):
            df_data = df[columns].astype(float)
            display_data = df_data.iloc[0,:].to_list()
            display_data.append(sum(display_data))
            display_data[:] = [round(float(x),2) for x in display_data]

            display_name = df_data.columns.to_list()
            display_name.append(sub_total_name)
            if sub_total_color is None:
                sub_total_color = 'orange' if float(display_data[3]) >= 0 else 'green'
            measure = ['total' if i == (len(display_data)-1) else 'relative' for i in range(len(display_data))]

            chart = (
                cls.go.Figure(
                    cls.go.Waterfall(
                        name=chart_title,
                        measure=measure,
                        x=display_name,
                        y=display_data,
                        textposition=textposition,
                        text=display_data,
                        textfont=dict(size=font_size, family=font_style),
                        connector={"line": {"color": "rgb(63, 63, 63)", "dash": dash_style}},
                        increasing = {"marker":{"color":positive_color}},
                        decreasing = {"marker":{"color":negative_color}},
                        totals = {"marker":{"color":sub_total_color}},
                    )
                )
            )
            chart.update_layout(
                title = {
                    'text': chart_title,
                    'font': {'size': title_size, 'family': font_style}
                },
                xaxis = {
                    'title': {
                        'text': xaxis_name,
                        'font': {'size': xaxis_size, 'family': font_style}
                    },
                    'tickfont': {'size': x_tick_size, 'family': font_style}
                },
                yaxis = {
                    'title': {
                        'text': yaxis_name,
                        'font': {'size': yaxis_size, 'family': font_style}
                    },
                    'tickfont': {'size': y_tick_size, 'family': font_style}
                },
                # showlegend = True,

            )
            chart.add_shape(
            type="line", line=dict(color=dash_color, dash=dash_style), opacity=0.5,
            x0=-0.4, x1=len(display_data)-1+0.4, xref="x", y0=chart.data[0].y[0], y1=chart.data[0].y[0]+0.001, yref="y"
            )
            chart.add_shape(
            type="line", line=dict(color=dash_color, dash=dash_style), opacity=0.5,
            x0=-0.4, x1=len(display_data)-1+0.4, xref="x", y0=chart.data[0].y[-1], y1=chart.data[0].y[-1]+0.001, yref="y"
            )
            chart.add_shape(
            type="line", line=dict(color=zero_line), opacity=1,
            x0=-0.4, x1=len(display_data)-1+0.4, xref="x", y0=0.0, y1=0.001, yref="y"
            )
            return chart
        
    @classmethod
    def _to_list(cls, value):
        result = []
        value_type = type(value)
        if value_type is list:
            return value
        else:
            if value_type is int or value_type is str:
                result.append(value)
                return result
            else:
                return None

    @classmethod
    def full_outer_join_2_list(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list = lista
        idxa = 0
        idxb = 0
        lena = len(lista)
        lenb = len(listb)
        while idxa < lena and idxb < lenb:
            if lista[idxa] == listb[idxb]:
                idxb += 1
                idxa = 0
            else:
                if idxa == lena - 1:
                    joined_list.append(listb[idxb])
                    idxb += 1
                    idxa = 0
                else:
                    idxa += 1
        return joined_list
    
    @classmethod
    def inner_join_2_list(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list = []
        idxa = 0
        idxb = 0
        lena = len(lista)
        lenb = len(listb)
        while idxa < lena and idxb < lenb:
            if lista[idxa] == listb[idxb]:
                joined_list.append(listb[idxb])
                idxb += 1
                idxa = 0
            else:
                if idxa == lena - 1:
                    idxb += 1
                    idxa = 0
                else:
                    idxa += 1
        return joined_list
    
    @classmethod
    def non_outer_join_2_list(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list1 = cls.non_outer_join_a_vs_b(a = listb, b = lista)
        joined_list2 = cls.non_outer_join_a_vs_b(a = lista, b = listb)
        combined_list = joined_list1 + joined_list2
        return combined_list

    @classmethod
    def non_outer_join_a_vs_b(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list = []
        idxa = 0
        idxb = 0
        lena = len(lista)
        lenb = len(listb)
        while idxa < lena and idxb < lenb:
            if lista[idxa] == listb[idxb]:
                idxb += 1
                idxa = 0
            else:
                if idxa == lena - 1:
                    joined_list.append(listb[idxb])
                    idxb += 1
                    idxa = 0
                else:
                    idxa += 1
        return joined_list

    @classmethod
    def extract_sheet_id(cls, sheet_url):
        start_index = sheet_url.find("/d/")
        if start_index != -1:
            start_index += 3  # Move to the character after "/d/"
            end_index = sheet_url.find("/", start_index)  # Find the next "/"
            if end_index == -1:
                sheet_id = sheet_url[start_index:]
            else:
                sheet_id = sheet_url[start_index:end_index]
            return sheet_id
        else:
            return None

    @staticmethod
    def extract_google_drive_id(url):
        # Define the patterns for Google Drive documents
        google_drive_patterns = [
            # Common document formats
            "/spreadsheets/d/",
            "/document/d/",
            "/presentation/d/",
            "/file/d/",
            "/forms/d/",
            "/drawings/d/",
            
            # Folders and drives
            "/drive/folders/",
            "/drive/u/[0-9]+/folders/",
            "/drive/shared-drives/",
            "/drive/u/[0-9]+/shared-drives/",
        ]

        # Try to match standard patterns
        for pattern in google_drive_patterns:
            # Use regex to handle patterns with wildcards like u/[0-9]+
            if '[0-9]+' in pattern:
                pattern_regex = pattern.replace('[0-9]+', '[0-9]+')
                match = re.search(f"{pattern_regex}([^/?]+)", url)
                if match:
                    return match.group(1)
            else:
                start_index = url.find(pattern)
                if start_index != -1:
                    start_index += len(pattern)  # Move index to start of the ID
                    end_index = url.find("/", start_index)  # Find the next "/"
                    if end_index == -1:  # If no "/" found
                        end_index = url.find("?", start_index)  # Try finding a "?"
                        if end_index == -1:  # If no "?" found either
                            return url[start_index:]  # Take the rest as ID
                        else:
                            return url[start_index:end_index]  # Take until "?"
                    else:
                        return url[start_index:end_index]  # Take until "/"
        
        # Handle Google Apps Script URLs
        script_match = re.search(r'script\.google\.com/[a-z/]+/([^/?]+)', url)
        if script_match:
            return script_match.group(1)

        return None
    
    @classmethod
    def _read_csv(cls, file_path):
        try:
            with open(file_path, mode = 'r', newline='') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
                if rows:
                    return rows
                    # last_row = rows[-1]
                    # return int(last_row['row_number'])
        except Exception as e:
            print(e)
            return None

    @classmethod
    def _write_csv(cls, file_path, mode, data_to_write):
        with open(file_path, mode=mode, newline='', encoding="utf-8", errors="ignore") as file:
            writer = csv.writer(file)
            writer.writerow(cls._to_list(data_to_write))
    
    @classmethod
    def _write_csv_log(cls, file_path, key_column, *args):
        _data = list(args)
        _data.insert(1, cls._get_current_time('string'))
        # _data = args + (cls._get_current_time('string'),) #add write time
        data = _data
        if os.path.exists(file_path):
            all_rows = cls._read_csv(file_path=file_path)
            if all_rows is None:
                print('error getting last row number')
                return
            last_row = all_rows[-1]
            last_row_num = int(last_row['row_number'])
            row_number = last_row_num + 1
        else:
            column_names = []
            try:
                data_len = len(data)
            except Exception as e:
                print(e)
                return
            if data_len == 2:
                pass
            else:
                key = data[:2]
                non_key = data[2:]
                non_key_columns = cls.non_outer_join_a_vs_b(key, non_key)
                for column in non_key_columns:
                    column_name = input(f'Enter column name for column ({column}): ')
                    column_names.append(column_name)
            column_names_to_write = cls._to_list('row_number') + cls._to_list(key_column) + cls._to_list('write_time') + column_names
            mode = 'w'
            row_number = 1
            cls._write_csv(file_path=file_path, mode=mode, data_to_write=column_names_to_write)
        data_to_write = cls._to_list(row_number) + cls._to_list(data)
        try:
            cls._write_csv(file_path=file_path, mode='a', data_to_write=data_to_write)
        except Exception as e:
            print(e)

    @classmethod
    def sheet_id_to_url(cls,sheets_id):
        base_url = "https://docs.google.com/spreadsheets/d/"
        return f"{base_url}{sheets_id}/edit"
    
    @classmethod
    def check_dup_df_column_name(cls,df):
        column_list = df.columns.tolist()
        dup_columns = pd.DataFrame({'column':column_list}).groupby('column').agg(count=('column','count')).reset_index().query('count>1').column.tolist()
        if len(dup_columns) == 0:
            return
        for column in dup_columns:
            print(column)
    
    @classmethod
    def procedure_and_logging(cls, log_path, input_list, main_func, key_column='key_column', *args):
        input_list_number = ''
        while True:
            input_limit = input('Perform for all input? (y/n): ')
            if input_limit == 'y':
                break
            elif input_limit == 'n':
                while True:
                    limit = input('Number of input to run: ')
                    try:
                        input_list_number = int(limit)
                    except:
                        print('Please enter a number')
                        continue
                    break
                break
            else:
                continue
        
        if input_list_number == '':
            pass
        else:
            input_list = input_list[:input_list_number]
        
        _input_list = [str(i) if isinstance(i, bytes) else i for i in input_list]
        input_type = type(input_list[0])

        if os.path.exists(log_path):
            all_logs = cls._read_csv(file_path=log_path)
            if all_logs is None:
                return
            df_all_logs = pd.DataFrame(all_logs)
            completed_procedure = df_all_logs[key_column].to_list()
            _remaining_procedure = MyFunction.non_outer_join_a_vs_b(a=completed_procedure, b=_input_list)
            if input_type is bytes:
                remaining_procedure = [i[2:-1].encode('ascii') for i in _remaining_procedure]
            else: remaining_procedure = _remaining_procedure
        else:
            remaining_procedure = input_list
        for procedure in remaining_procedure:
            data = main_func(procedure, *args)
            if data is None:
                continue
            if isinstance(data, tuple):
                try:
                    cls._write_csv_log(log_path, key_column, *data)
                except Exception as e:
                    print('Error writing 01:', e)
            else:
                try:
                    cls._write_csv_log(log_path, key_column, data)
                except Exception as e:
                    print('Error writing 02:', e)

    @classmethod
    def _get_current_time(cls, type: Literal['date', 'time', 'datetime']='datetime'):
        timezone = pytz.timezone('Asia/Bangkok')
        current_time = datetime.now(timezone)
        checker = True
        while checker == True:
            if type == 'date':
                current_date_time = current_time.strftime("%Y-%m-%d")
                break
            elif type == 'time':
                current_date_time = current_time.strftime("T%H:%M:%S")
                break
            elif type == 'datetime':
                current_date_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
                break
            else:
                checker = False
                current_date_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        return current_date_time


    @classmethod
    def _remove_accents(cls, input_str):
        s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
        s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'
        s = ''
        for c in input_str:
            if c in s1:
                s += s0[s1.index(c)]
            else:
                s += c
        return s
    
    @classmethod
    def _remove_strange_symbols(cls, input_str):
        import re
        pattern = re.compile(r'[^a-zA-Z0-9\s]')
        filtered_data = [pattern.sub('', item) for item in input_str]
        return filtered_data
    
    @classmethod
    def _remove_other_symbols(cls, input_str):
        replacements = {'\n': ' ', '\r': '', '“': '', '”':'', '₫':''} 
        body = ''.join(replacements.get(c, c) for c in input_str)
        return body
        
    @classmethod
    def _transform_column_name_first_letter(cls, column_name):
        if column_name and column_name[0].isdigit():
            return 'n' + column_name
        else:
            return column_name
    
    @classmethod
    def _make_column_name_unique(cls, df):
        seen_names = set()
        new_columns = []
        for column in df.columns:
            new_column = column
            i = 2
            while new_column in seen_names:
                new_column = f"{column}_{i}"
                i += 1
            seen_names.add(new_column)
            new_columns.append(new_column)

        df.columns = new_columns

    @classmethod
    def standadize_column_name(cls,df):
        df.columns = (
            df.rename(columns={'':'blank'})
            .columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('-', '')
            .str.replace('(', '')
            .str.replace(')', '')
            .str.replace('?', '')
            .str.replace('!', '')
            .str.replace('@', '')
            .str.replace('$', '')
            .str.replace('^', '')
            .str.replace('*', '')
            .str.replace(':', '')
            .str.replace('.', '_')
            .str.replace('&', 'and')
            .str.replace('%', 'percent')
            .str.strip()
        )
        df.columns = df.columns.map(cls._remove_accents)
        df.columns = df.columns.map(cls._transform_column_name_first_letter)
        cls._make_column_name_unique(df)

    @staticmethod
    def execute_function_and_log(func, success_message=None, failed_message=None, log_success_execution=False, path=None, log_name=None):
        import logging
        if path:
            if log_name:
                file_name = path+'/'+log_name
            else:
                file_name = path+'/app.log'
        else:
            if log_name:
                file_name = log_name
            else:
                file_name = 'app.log'

        logging.basicConfig(level=logging.DEBUG, filename=file_name, filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s')
        try:
            func
            if log_success_execution:
                logging.info(f'Successfully {success_message}')
            else:
                pass
        except Exception as e:
            logging.error(f'Error {failed_message}', exc_info=True)

    
    @staticmethod
    def _get_input_num_type(message):
        while True:
            limit = input(f'{message}')
            try:
                input_number = int(limit)
            except:
                print('Please enter a number')
                continue
            break
        return input_number
    
    @staticmethod
    def _extract_email(text):
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(pattern, text)
        if match:
            return match.group()
        else:
            return None
        
    @staticmethod
    def _string_replace(string, **kwargs):
        result = ''.join(kwargs.get(c, c) for c in string)
        return result
    
    @staticmethod
    def convert_long_date_to_gmt7(date_string):
        date_string = date_string.split('(')[0].strip()
        time_adjust = int(date_string[-5:-2])
        
        date_format = "%a, %d %b %Y %H:%M:%S %z"
        dt = datetime.strptime(date_string, date_format)
        dt_gmt_plus_7 = dt + timedelta(hours=(7-time_adjust))
        result = dt_gmt_plus_7.strftime("%a, %d %b %Y %H:%M:%S")
        return result
    
    @staticmethod
    def list_to_single_string(input, delimiter=None, wrapper = None):
        if isinstance(input,(list,tuple)):
            pass
        else:
            print('input must be a list or tuple')
        try:
            single_string = delimiter.join(f"{wrapper}{item}{wrapper}" for item in input)
        except Exception as e:
            print('error: ', e)
            return
        return single_string
"""create web server with ngrok service"""
# !pip install Flask==3.0.0 pyngrok==7.1.2 --quiet
# from flask import Flask, request
# from pyngrok import ngrok
# class WebServer:
#     def __init__(self):
#         self.ngrok_key = '2e5LT2zgYwFyeqwyuQstRF1H9VB_7ygrgxJ5qPDtmkQXAucUa'
#         self.port = 5000
#         self.session_duration = 100
#         self.token = token
#         self.attempts = 0

#     def webserver_ngrok(self):
#         ngrok.set_auth_token(self.ngrok_key)
#         app = Flask(__name__)
#         @app.route("/", methods=['GET', 'POST'])
#         def user_interface():
#         while self.attempts <=3:
#             if self.attempts == 3:
#             with open("user_input.txt", "w") as file:
#                 file.write(termination_token)
#             return f'You have entered too many incorrect token, this session is now terminated!'
#             if request.method == "POST":
#                 user_input = request.form["user_input"]
#                 if user_input != self.token:
#                 self.attempts+=1
#                 return f'Incorrect token, please reload and re-enter.{self.attempts}'
#                 # Write the input to a file
#                 with open("user_input.txt", "w") as file:
#                     file.write(user_input)
#                 print(f"Input received: {user_input}")  # Attempt to print to Colab's output
#                 return f"Received input: {user_input}"
#             # If it's a GET request, show the form
#             return '''
#                 <form method="post">
#                     Enter Verification Code: <input type="text" name="user_input"><br>
#                     <input type="submit" value="Submit">
#                 </form>
#             '''
#         return app

#     def create_ngrok_tunnel(self):
#         tunnel = ngrok.connect(self.port)
#         return tunnel

#     def disconnect_webserver(self, url):
#         ngrok.disconnect(url)

#     def create_webserver_thread(self):
#         import threading
#         app = self.webserver_ngrok()
#         tunnel = self.create_ngrok_tunnel()
#         webserver_url = tunnel.public_url
#         thread_ngrok = threading.Thread(target=lambda: app.run(port=self.port, use_reloader=False))
#         thread_ngrok.start()
#         timer = threading.Timer(self.session_duration, self.disconnect_webserver, args=(webserver_url,))
#         timer.start()
#         return webserver_url

#     def disconnect_ngrok():
#     ngrok.disconnect(url)

#     def temp_test():
#     # Set the ngrok auth token
#     ngrok.set_auth_token(ngrok_key)

#     app = Flask(__name__)

#     # Route to display the form and handle input
#     @app.route("/", methods=["GET", "POST"])
#     def hello():
#         if request.method == "POST":
#             user_input = request.form["user_input"]
#             # Write the input to a file
#             with open("user_input.txt", "w") as file:
#                 file.write(user_input)
#             print(f"Input received: {user_input}")  # Attempt to print to Colab's output
#             return f"Received input: {user_input}"
#         # If it's a GET request, show the form
#         return '''
#             <form method="post">
#                 Enter Token Code: <input type="text" name="user_input"><br>
#                 <input type="submit" value="Submit">
#             </form>
#         '''

#     # Start ngrok tunnel
#     tunnel = ngrok.connect(port)
#     public_url = tunnel.public_url
#     print(f"ngrok tunnel \"{public_url}\" -> \"http://localhost:{port}\"")

#     # Write the ngrok URL to a file
#     # with open("ngrok_url.txt", "w") as f ile:
#     #     file.write(public_url)

#     # Optionally, display the URL in Python code or notebook cell
#     print(f"Access the web server via: {public_url}")

#     # Start Flask app in a way that allows you to continue executing other commands
#     # Note: The use_reloader=False is to avoid running the initialization code twice in development mode
#     import threading
#     ngrok_thread = threading.Thread(target=lambda: app.run(port=port, use_reloader=False))
#     ngrok_thread.start()
#     timer = threading.Timer(100, disconnect_ngrok, args=(public_url,))
#     timer.start()

#     return public_url

class MyProject:

    class MyProjectProperty:
        def __init__(self):
            counter = 0
            while counter <= 3:
                counter += 1
                email_address = input('Email address: ')
                email_app_password = input('Email app password: ')
                mail = GoogleMail(email_address=email_address, password=email_app_password)
                try:
                    mail.get_email_ids()[1]
                    break
                except Exception as e:
                    print(e)
                    if counter == 3:
                        print('wrong, can only try 3 times')
                        return
                    else:
                        print('wrong email or password, try again')
                    continue
            self._mail = mail
    
        @property
        def mail(self):
            return self._mail

    class MainProject:
        def __init__(self) -> None:
            try:
                self.mail = MyProject.MyProjectProperty().mail
            except Exception as e:
                print(f'error: {e}')
            pass

        def _check_login(self):
            try:
                a = self.mail
            except:
                a = None
            if a:
                return self.mail
            else:
                return None
            
        def _get_bank_email(self, id, bank_email_list):
            mail = self._check_login()
            if mail:
                pass
            else: return

            raw_result = self.mail.fetch_email_by_id(id)
            # print(result[2], id)
            # print(bank_email_list)
            # print(result[2] in bank_email_list)
            if raw_result[2] in bank_email_list:
                try:
                    body = self.mail._extract_bank_body(raw_result[2], raw_result[-1])
                except:
                    print('problem', id)
                
                result = list(raw_result[:-1])
                result.append(body)
                return result
            else: return None

        def _get_bank_notification(self, log_path, *args: Literal['input sender email of banks']):
            mail = self._check_login()

            if mail:
                pass
            else:
                return
            email_num = MyFunction._get_input_num_type(message='How many email to retrieve?')

            try:
                email_id_list = self.mail.get_email_ids()[-email_num:]
            except Exception as e:
                print('error getting email ids with error: {}'.format(e))
                return
            
            MyFunction.procedure_and_logging(
                f'{log_path}\log.csv',
                email_id_list,
                self._get_bank_email,
                'key_column',
                args
            )


            # print(email_id_list)
            self.mail._log_out_email()

    ##a test here

class ImprovedBackgroundSelenium:
    def __init__(self, session_duration_minutes=60, log_file='selenium_background.log', cookies_path='cookies.pkl'):
        # Setup logging with more detailed information
        self.setup_enhanced_logging(log_file)
        
        # Session management
        self.session_duration = timedelta(minutes=session_duration_minutes)
        self.session_start_time = None
        self.session_end_time = None
        self.is_running = False
        self.thread = None
        self.driver = None
        self.stop_event = threading.Event()
        self.task_queue = []
        self.queue_lock = threading.Lock()
        self.queue_thread = None
        self.queue_running = False

        # Cookies configuration
        self.cookies_path = cookies_path
        self.cookies_loaded = False
        
        # Register cleanup handlers
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Store session info
        self.session_info_file = 'selenium_session.json'
        
        # Lock for thread-safe operations
        self._driver_lock = threading.Lock()
        
        # Flag for forced termination
        self._force_termination = False

    def setup_enhanced_logging(self, log_file):
        import logging
        """Setup improved logging configuration"""
        self.logger = logging.getLogger('background_selenium')
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers if any
        if self.logger.handlers:
            for handler in self.logger.handlers:
                self.logger.removeHandler(handler)
        
        # File handler with rotation
        import logging.handlers
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=3
        )
        file_handler.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] [%(levelname)s] - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def initialize_driver(self):
        """Initialize the Selenium WebDriver with improved options"""
        self.logger.info("Initializing Chrome WebDriver...")
        
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Create a persistent Chrome profile directory
        user_data_dir = os.path.join(os.getcwd(), "chrome_profile")
        if not os.path.exists(user_data_dir):
            os.makedirs(user_data_dir)
        
        # Add the user data directory to Chrome options
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--profile-directory=Default")
        
        # Additional options to improve session persistence and avoid detection
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-site-isolation-trials")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Set up preferences including download directory
        download_dir = os.path.join(os.getcwd(), "downloads")
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            
        prefs = {
            "credentials_enable_service": True,
            "profile.password_manager_enabled": True,
            "autofill.profile_enabled": True,
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        options.add_experimental_option("prefs", prefs)
        
        # Create driver
        try:
            with self._driver_lock:
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=options
                )
                
                # Additional anti-detection measures
                self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    """
                })
                
                self.logger.info("Chrome WebDriver initialized successfully")
                return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome WebDriver: {e}", exc_info=True)
            return False

    def get_to_site(self, url, driver=None, stop_event=None):
        """Enhanced navigation with improved login handling"""
        try:
            self.logger.info(f'Navigating to {url}...')
            driver = driver if driver is not None else self.driver
            
            # Screenshot before any action
            driver.save_screenshot("pre_navigation.png")
            self.logger.info("Saved pre-navigation screenshot")
            
            # Try to load cookies first
            cookies_loaded = self.load_cookies(path=self.cookies_path, url=url)
            self.cookies_loaded = cookies_loaded
            
            # Navigate to the URL
            driver.get(url)
            self.logger.info(f'Initial navigation complete. Current URL: {driver.current_url}')
            
            # Take screenshot after navigation
            driver.save_screenshot("post_navigation.png")
            self.logger.info("Saved post-navigation screenshot")
            
            # Check if we need to log in - more comprehensive detection
            login_indicators = [
                "login", "signin", "sign in", "auth", "account/auth", 
                "username", "password", "email", "log in"
            ]
            
            # Check URL and page source
            url_needs_login = any(indicator in driver.current_url.lower() for indicator in login_indicators)
            content_needs_login = any(indicator in driver.page_source.lower() for indicator in login_indicators)
            
            # Also check for login form elements
            login_elements = False
            try:
                # Look for common login form elements
                login_form_elements = driver.find_elements(By.XPATH, 
                    "//input[@type='password'] | //input[@name='password'] | "
                    "//form[contains(., 'login') or contains(., 'sign in')] | "
                    "//button[contains(., 'Login') or contains(., 'Sign in')]"
                )
                login_elements = len(login_form_elements) > 0
                
                if login_elements:
                    self.logger.info(f"Found {len(login_form_elements)} login-related elements")
            except Exception as e:
                self.logger.warning(f"Error checking for login elements: {e}")
            
            needs_login = url_needs_login or content_needs_login or login_elements
                
            if needs_login:
                self.logger.info("Login page detected, initiating login flow")
                
                # Take screenshot of login page
                driver.save_screenshot("login_page.png")
                self.logger.info("Saved login page screenshot")
                
                # Try automated login if credentials are configured
                login_successful = False
                if hasattr(self, 'username') and hasattr(self, 'password') and self.username and self.password:
                    self.logger.info("Attempting automated login with configured credentials")
                    login_successful = self.attempt_login(driver)
                
                if not login_successful:
                    # Wait for manual login
                    self.logger.info("=== MANUAL LOGIN REQUIRED ===")
                    print("\n" + "="*70)
                    print("LOGIN REQUIRED: Please log in manually in the browser window.")
                    print("The automation will continue once you've completed login.")
                    print("="*70 + "\n")
                    
                    # Wait for login to complete with progress updates
                    max_wait = 300  # 5 minutes max wait time
                    wait_interval = 5  # Check every 5 seconds
                    total_waited = 0
                    
                    while needs_login and total_waited < max_wait:
                        if stop_event and stop_event.is_set():
                            self.logger.info("Stop event detected during login wait")
                            return
                            
                        try:
                            time.sleep(wait_interval)
                            total_waited += wait_interval
                            
                            # Check URL again
                            current_url = driver.current_url
                            url_still_login = any(indicator in current_url.lower() for indicator in login_indicators)
                            
                            # Check page source again
                            try:
                                content_still_login = any(indicator in driver.page_source.lower() for indicator in login_indicators[:4])  # Using only the first few indicators for speed
                            except:
                                content_still_login = False
                                
                            # Try to find elements that indicate successful login
                            success_indicators = [
                                "//button[contains(., 'Log out') or contains(., 'Sign out')]",
                                "//a[contains(., 'Log out') or contains(., 'Sign out')]",
                                "//div[contains(@class, 'user-info')]",
                                "//span[contains(@class, 'username')]"
                            ]
                            
                            success_elements = False
                            for indicator in success_indicators:
                                try:
                                    elements = driver.find_elements(By.XPATH, indicator)
                                    if elements:
                                        success_elements = True
                                        break
                                except:
                                    pass
                            
                            # Check if we're no longer at login page
                            if (not url_still_login) or success_elements or (not content_still_login):
                                self.logger.info("Login seems complete based on page changes")
                                needs_login = False
                                break
                                
                            print(f"Waiting for login... ({total_waited} seconds elapsed)")
                                
                        except Exception as e:
                            self.logger.warning(f"Error while waiting for login: {e}")
                    
                    if needs_login:
                        self.logger.warning("Login timeout reached or failed detection. Proceeding anyway.")
                        # Take screenshot of the state
                        driver.save_screenshot("login_timeout.png")
                    else:
                        self.logger.info("Login completed successfully")
                        driver.save_screenshot("login_success.png")
                        # Save the cookies for future use
                        self.save_cookies(path=self.cookies_path)
                        
                        # Reload the target URL for fresh start
                        driver.get(url)
                        self.logger.info(f"Reloaded page after login. Current URL: {driver.current_url}")
            
            # Wait for the page to load after navigation
            try:
                # Wait for common page elements with improved timeout handling
                wait = WebDriverWait(driver, 30)
                
                # Try different common elements
                for selector in [
                    (By.TAG_NAME, "body"),  # Basic element
                    (By.TAG_NAME, "header"),  # Common header
                    (By.TAG_NAME, "main"),   # Main content
                    (By.CSS_SELECTOR, ".app-content"),  # Common app content
                    (By.CSS_SELECTOR, "#content"),  # Common content div
                ]:
                    try:
                        wait.until(EC.presence_of_element_located(selector))
                        self.logger.info(f"Found element {selector}")
                        break
                    except:
                        continue
                        
                self.logger.info("Page loaded successfully")
                
            except Exception as e:
                self.logger.warning(f"Timeout or error waiting for page elements: {e}")
                
            # Take final screenshot
            try:
                screenshot_path = os.path.join(os.getcwd(), "navigation_complete.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Final screenshot saved to {screenshot_path}")
            except Exception as e:
                self.logger.warning(f"Failed to take screenshot: {e}")
                
            # Print final status
            self.logger.info(f'Navigation complete. Final URL: {driver.current_url}')
            
        except Exception as e:
            self.logger.error(f'Error navigating to URL {url}: {e}', exc_info=True)
            try:
                driver.save_screenshot("navigation_error.png")
                self.logger.info("Saved error screenshot")
            except:
                pass

    def attempt_login(self, driver):
        """Attempt automated login if credentials are available"""
        try:
            # Look for username/email field
            username_selectors = [
                "//input[@name='username']",
                "//input[@name='email']",
                "//input[@id='username']",
                "//input[@id='email']",
                "//input[@placeholder='Username' or @placeholder='Email']",
                "//input[@type='text']"
            ]
            
            # Look for password field
            password_selectors = [
                "//input[@name='password']",
                "//input[@id='password']",
                "//input[@placeholder='Password']",
                "//input[@type='password']"
            ]
            
            # Find username field
            username_field = None
            for selector in username_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        username_field = elements[0]
                        break
                except:
                    continue
                    
            # Find password field
            password_field = None
            for selector in password_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        password_field = elements[0]
                        break
                except:
                    continue
            
            if not username_field or not password_field:
                self.logger.warning("Could not locate username or password fields")
                return False
                
            # Fill in credentials
            self.logger.info("Found login fields, filling credentials")
            username_field.clear()
            username_field.send_keys(self.username)
            
            password_field.clear()
            password_field.send_keys(self.password)
            
            # Find login button
            login_button_selectors = [
                "//button[contains(., 'Login') or contains(., 'Sign in') or contains(., 'Log in')]",
                "//button[@type='submit']",
                "//input[@type='submit']",
                "//button[contains(@class, 'login') or contains(@class, 'signin')]",
                "//button"  # Last resort: any button
            ]
            
            login_button = None
            for selector in login_button_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        login_button = elements[0]
                        break
                except:
                    continue
            
            if not login_button:
                self.logger.warning("Could not locate login button")
                
                # Try to submit the form directly
                try:
                    self.logger.info("Attempting to submit form directly")
                    form = driver.find_element(By.XPATH, "//form")
                    form.submit()
                    time.sleep(2)
                except:
                    self.logger.warning("Form submission failed, login incomplete")
                    return False
            else:
                # Click the login button
                self.logger.info(f"Clicking login button: {login_button.text}")
                try:
                    login_button.click()
                except:
                    # Try JavaScript click as fallback
                    try:
                        driver.execute_script("arguments[0].click();", login_button)
                        self.logger.info("Used JavaScript to click login button")
                    except:
                        self.logger.warning("Button click failed, login incomplete")
                        return False
            
            # Wait briefly to see if login worked
            time.sleep(5)
            
            # Check for login errors or success
            if "incorrect" in driver.page_source.lower() or "invalid" in driver.page_source.lower():
                self.logger.warning("Login failed - error message detected on page")
                return False
                
            # Look for success indicators
            success_indicators = [
                "//button[contains(., 'Log out') or contains(., 'Sign out')]",
                "//a[contains(., 'Log out') or contains(., 'Sign out')]",
                "//div[contains(@class, 'user-info')]",
                "//span[contains(@class, 'username')]"
            ]
            
            for indicator in success_indicators:
                try:
                    elements = driver.find_elements(By.XPATH, indicator)
                    if elements:
                        self.logger.info("Login successful - found success indicator")
                        return True
                except:
                    pass
            
            # Check if URL changed away from login page
            login_indicators = ["login", "signin", "auth"]
            if not any(indicator in driver.current_url.lower() for indicator in login_indicators):
                self.logger.info("Login seems successful based on URL change")
                return True
                
            # If we're not sure, log the status but return True to continue
            self.logger.info("Login status uncertain, but proceeding")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during login attempt: {e}", exc_info=True)
            return False

    def save_cookies(self, path=None):
        """Save current browser cookies to a file"""
        if path is None:
            path = self.cookies_path
            
        try:
            if self.driver:
                with self._driver_lock:
                    cookies = self.driver.get_cookies()
                with open(path, 'wb') as file:
                    pickle.dump(cookies, file)
                self.logger.info(f"Cookies saved to {path}")
                return True
            else:
                self.logger.error("No driver available to save cookies")
                return False
        except Exception as e:
            self.logger.error(f"Error saving cookies: {e}", exc_info=True)
            return False

    def load_cookies(self, path=None, url=None):
        """Load cookies from file into current browser session"""
        if path is None:
            path = self.cookies_path
            
        try:
            if not self.driver:
                self.logger.error("No driver available to load cookies")
                return False
                
            # Navigate to the domain first if provided
            if url:
                try:
                    base_url = url.split('//', 1)[1].split('/', 1)[0]  # Extract domain
                    with self._driver_lock:
                        self.driver.get(f"https://{base_url}")
                    self.logger.info(f"Navigated to base domain: {base_url}")
                except:
                    self.logger.warning(f"Could not extract domain from URL: {url}")
            
            if not os.path.exists(path):
                self.logger.warning(f"Cookie file {path} not found")
                return False
                
            with open(path, 'rb') as file:
                cookies = pickle.load(file)
                
            # Add each cookie with error handling for individual cookies
            cookie_success = 0
            cookie_fail = 0
            with self._driver_lock:
                for cookie in cookies:
                    try:
                        # Ensure cookie is compatible (remove problematic attributes)
                        if 'expiry' in cookie:
                            cookie['expiry'] = int(cookie['expiry'])
                        
                        self.driver.add_cookie(cookie)
                        cookie_success += 1
                    except Exception as e:
                        self.logger.debug(f"Error adding cookie: {e}")
                        cookie_fail += 1
            
            self.logger.info(f"Cookies loaded: {cookie_success} successful, {cookie_fail} failed")
            
            # Refresh to apply cookies
            with self._driver_lock:
                self.driver.refresh()
            self.cookies_loaded = True
            return True
        except Exception as e:
            self.logger.error(f"Error loading cookies: {e}", exc_info=True)
            self.cookies_loaded = False
            return False

    def start_queue_processor(self):
        """Start a background thread that processes tasks from the queue with enhanced session management"""
        if self.queue_thread and self.queue_thread.is_alive():
            self.logger.warning("Task queue is already running")
            return False
            
        if not self.initialize_driver():
            self.logger.error("Failed to initialize driver for task queue")
            return False
            
        # Set up session timing
        self.session_start_time = datetime.now()
        self.session_end_time = self.session_start_time + self.session_duration
        
        # Reset termination flag
        self._force_termination = False
        
        # Save session info
        self.save_session_info()
        
        self.queue_running = True
        self.is_running = True
        self.stop_event.clear()  # Ensure the stop event is cleared
        
        self.queue_thread = threading.Thread(
            target=self._process_queue_with_expiration,
            name="QueueProcessor",
            daemon=True
        )
        self.queue_thread.start()
        self.logger.info(f"Task queue processor started. Session will expire at {self.session_end_time}")
        return True
        
    def _process_queue_with_expiration(self):
        """Process tasks from the queue with improved termination handling"""
        try:
            self.logger.info("Queue processor thread started")
            
            while self.queue_running and not self.stop_event.is_set() and not self._force_termination:
                # Check for session expiration
                if datetime.now() >= self.session_end_time:
                    self.logger.info("Session has expired.")
                    break
                    
                task = None
                with self.queue_lock:
                    if self.task_queue:
                        task = self.task_queue.pop(0)
                        
                if task:
                    func, args, kwargs = task
                    try:
                        # Add standard parameters if not already provided
                        if 'driver' not in kwargs or kwargs['driver'] is None:
                            kwargs['driver'] = self.driver
                        if 'stop_event' not in kwargs or kwargs['stop_event'] is None:
                            kwargs['stop_event'] = self.stop_event
                            
                        self.logger.info(f"Executing task: {func.__name__}")
                        function_start = time.time()
                        func(*args, **kwargs)
                        function_duration = time.time() - function_start
                        self.logger.info(f"Task {func.__name__} completed in {function_duration:.2f} seconds")
                    except Exception as e:
                        self.logger.error(f"Error executing task {func.__name__}: {e}", exc_info=True)
                        # Take a screenshot for troubleshooting
                        with self._driver_lock:
                            if self.driver:
                                screenshot_path = os.path.join(os.getcwd(), f"task_error_{func.__name__}.png")
                                try:
                                    self.driver.save_screenshot(screenshot_path)
                                    self.logger.info(f"Error screenshot saved to {screenshot_path}")
                                except:
                                    pass
                else:
                    # No tasks, sleep briefly
                    time.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Fatal error in queue processor: {e}", exc_info=True)
        finally:
            # Clean up resources
            self.logger.info("Queue processor stopping, cleaning up resources")
            try:
                self.cleanup_driver()
            except Exception as e:
                self.logger.error(f"Error cleaning up driver: {e}")
                
            self.is_running = False
            self.queue_running = False
            
            # Clean up session info
            try:
                if os.path.exists(self.session_info_file):
                    os.remove(self.session_info_file)
                    self.logger.info("Session info removed")
            except Exception as e:
                self.logger.error(f"Failed to remove session info: {e}")
                
            self.logger.info("Queue processor thread terminated")

    def stop_queue(self):
        """Stop the task queue processor with enhanced termination"""
        if not self.queue_running:
            self.logger.warning("Task queue is not running")
            return False
            
        self.logger.info("Stopping task queue processor...")
        
        # Set both flags to ensure termination
        self.queue_running = False
        self.stop_event.set()
        self._force_termination = True
        
        # Wait for thread completion with timeout
        if self.queue_thread and self.queue_thread.is_alive():
            self.logger.info("Waiting for queue thread to terminate...")
            self.queue_thread.join(timeout=10)
            
            if self.queue_thread.is_alive():
                self.logger.warning("Queue thread did not terminate within timeout, forcing cleanup")
            
        # Clean up driver anyway
        try:
            self.cleanup_driver()
        except Exception as e:
            self.logger.error(f"Error during driver cleanup: {e}")
            
        self.is_running = False
        self.logger.info("Task queue processor stopped")
        return True

    def cleanup_driver(self):
        """Clean up the WebDriver resources with improved safety"""
        with self._driver_lock:
            if self.driver:
                try:
                    # Try graceful shutdown first
                    self.logger.info("Closing WebDriver...")
                    self.driver.close()
                    time.sleep(0.5)
                    
                    self.logger.info("Quitting WebDriver...")
                    self.driver.quit()
                    self.logger.info("WebDriver resources released")
                except Exception as e:
                    self.logger.error(f"Error closing WebDriver normally: {e}")
                    
                    # Try force termination as last resort
                    try:
                        import psutil
                        import os
                        current_pid = os.getpid()
                        current_process = psutil.Process(current_pid)
                        children = current_process.children(recursive=True)
                        
                        for child in children:
                            if "chrome" in child.name().lower():
                                self.logger.info(f"Force terminating Chrome process: {child.pid}")
                                child.terminate()
                    except Exception as kill_e:
                        self.logger.error(f"Error force-terminating Chrome: {kill_e}")
                finally:
                    self.driver = None

    def cleanup(self):
        """Clean up resources when the program exits"""
        self.logger.info("Performing final cleanup...")
        if self.is_running:
            self.stop()
        if self.queue_running:
            self.stop_queue()
        self.logger.info("Cleanup complete")
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {sig}, cleaning up...")
        self.cleanup()
        # Allow normal signal processing to continue
        signal.default_int_handler(sig, frame)

    def stop(self):
        """Enhanced method to stop the Selenium process"""
        if not self.is_running:
            self.logger.warning("No active Selenium process to stop")
            return False
        
        self.logger.info("Stopping Selenium background process...")
        self._force_termination = True
        self.stop_event.set()
        
        # Wait for the thread to complete (with timeout)
        if self.thread and self.thread.is_alive():
            self.logger.info("Waiting for thread to terminate...")
            self.thread.join(timeout=10)
            
            if self.thread.is_alive():
                self.logger.warning("Thread did not terminate within timeout, forcing cleanup")
        
        self.cleanup_driver()
        self.is_running = False
        self.logger.info("Selenium background process stopped")
        
        # Clean up session info
        if os.path.exists(self.session_info_file):
            try:
                os.remove(self.session_info_file)
                self.logger.info("Session info removed")
            except Exception as e:
                self.logger.error(f"Failed to remove session info: {e}")
        
        return True

    def save_session_info(self):
        """Save session information to a file"""
        try:
            import json
            session_info = {
                "session_start": self.session_start_time.isoformat() if self.session_start_time else None,
                "session_end": self.session_end_time.isoformat() if self.session_end_time else None,
                "is_running": self.is_running,
                "queue_running": self.queue_running,
                "tasks_count": len(self.task_queue),
                "pid": os.getpid(),
                "timestamp": datetime.now().isoformat()
            }
            
            with open(self.session_info_file, 'w') as f:
                json.dump(session_info, f, indent=2)
                
            self.logger.info("Session info saved")
            return True
        except Exception as e:
            self.logger.error(f"Error saving session info: {e}")
            return False

    # Add credentials for automated login
    def set_credentials(self, username, password):
        """Set username and password for automated login"""
        self.username = username
        self.password = password
        self.logger.info(f"Credentials set for user: {username}")

# Useful utility extensions
def wait_for_element(driver, locator_type, locator, timeout=10, condition=EC.element_to_be_clickable):
    """Utility function to wait for an element with proper error handling."""
    try:
        element = WebDriverWait(driver, timeout).until(
            condition((locator_type, locator))
        )
        return element
    except TimeoutException:
        driver.save_screenshot("element_wait_timeout.png")
        raise

def click_element(driver, locator_type, locator, timeout=10, description=None, retry_with_js=True):
    """Find and click an element with retry logic."""
    element_desc = description or f"{locator_type}='{locator}'"
    logging.info(f"Clicking element: {element_desc}")
    
    try:
        # Wait for element to be clickable
        element = wait_for_element(driver, locator_type, locator, timeout)
        
        # Try regular click
        try:
            element.click()
            logging.info(f"Successfully clicked element: {element_desc}")
            return True
        except Exception as e:
            logging.warning(f"Regular click failed for {element_desc}: {e}")
            
            # Try JavaScript click if enabled
            if retry_with_js:
                logging.info("Trying JavaScript click...")
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(0.5)  # Give time to scroll
                    driver.execute_script("arguments[0].click();", element)
                    logging.info(f"Successfully clicked element with JavaScript: {element_desc}")
                    return True
                except Exception as js_e:
                    logging.error(f"JavaScript click also failed: {js_e}")
                    driver.save_screenshot("js_click_error.png")
                    return False
            return False
    except Exception as e:
        logging.error(f"Error finding or clicking element {element_desc}: {e}")
        driver.save_screenshot("click_error.png")
        return False

def fill_element(driver, locator_type, locator, text, timeout=10, description=None, clear_first=True):
    """Find and fill an input element with text with improved error handling."""
    element_desc = description or f"{locator_type}='{locator}'"
    logging.info(f"Filling element {element_desc} with text: {text}")
    
    try:
        element = wait_for_element(driver, locator_type, locator, timeout, condition=EC.visibility_of_element_located)
        
        # First ensure element is in view
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(0.3)  # Brief pause after scrolling
        
        if clear_first:
            # Try different clearing methods
            try:
                element.clear()
            except:
                # Alternative clearing methods
                driver.execute_script("arguments[0].value = '';", element)
                # Or send Ctrl+A then Delete
                element.send_keys(u'\ue009' + 'a' + u'\ue009' + u'\ue017')
                
        # Try sending keys with potential retry
        try:
            element.send_keys(text)
        except Exception as e:
            logging.warning(f"Error with send_keys: {e}, trying JavaScript")
            # Try setting value via JavaScript
            try:
                driver.execute_script(f"arguments[0].value = '{text}';", element)
                # Trigger change event to ensure value is recognized
                driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", element)
            except Exception as js_e:
                logging.error(f"JavaScript value set also failed: {js_e}")
                driver.save_screenshot("fill_js_error.png")
                return False
                
        logging.info(f"Successfully filled element: {element_desc}")
        return True
    except Exception as e:
        logging.error(f"Error filling element {element_desc}: {e}")
        driver.save_screenshot("fill_error.png")
        return False

def select_dropdown_item(driver, dropdown_locator, dropdown_type, item_locator, item_type, 
                         dropdown_description=None, item_description=None, wait_time=2, timeout=10):
    """Select an item from a dropdown menu with improved robustness."""
    dropdown_desc = dropdown_description or f"{dropdown_type}='{dropdown_locator}'"
    item_desc = item_description or f"{item_type}='{item_locator}'"
    
    logging.info(f"Selecting {item_desc} from dropdown {dropdown_desc}")
    
    try:
        # Click the dropdown to open it
        dropdown_clicked = click_element(
            driver, 
            dropdown_type, 
            dropdown_locator,
            timeout=timeout,
            description=dropdown_desc
        )
        
        if not dropdown_clicked:
            logging.error(f"Failed to open dropdown {dropdown_desc}")
            return False
            
        # Wait for dropdown to expand
        time.sleep(wait_time)
        
        # Take screenshot of open dropdown for debugging
        driver.save_screenshot("dropdown_opened.png")
        
        # Check if the dropdown is actually opened
        try:
            # Look for dropdown container or menu items
            dropdown_opened = driver.find_elements(By.XPATH, 
                "//div[contains(@class, 'dropdown') and contains(@class, 'open')] | " +
                "//div[contains(@class, 'select-dropdown')] | " +
                "//div[contains(@class, 'popup')] | " +
                "//ul[contains(@class, 'dropdown-menu')]"
            )
            
            if not dropdown_opened:
                logging.warning("Dropdown may not be fully opened, retrying click")
                click_element(
                    driver, 
                    dropdown_type, 
                    dropdown_locator,
                    timeout=timeout,
                    description=dropdown_desc,
                    retry_with_js=True
                )
                time.sleep(wait_time)
        except:
            pass
        
        # Click the desired item
        item_clicked = click_element(
            driver,
            item_type,
            item_locator,
            timeout=timeout,
            description=item_desc,
            retry_with_js=True
        )
        
        if not item_clicked:
            logging.error(f"Failed to select item {item_desc} from dropdown")
            return False
            
        # Wait briefly for dropdown to close and selection to take effect
        time.sleep(1)
        
        logging.info(f"Successfully selected {item_desc} from dropdown {dropdown_desc}")
        return True
    except Exception as e:
        logging.error(f"Error during dropdown selection: {e}")
        driver.save_screenshot("dropdown_error.png")
        return False

def get_element_text(driver, locator_type, locator, timeout=10, default=None):
    """Get text from an element with error handling."""
    try:
        element = wait_for_element(driver, locator_type, locator, timeout, condition=EC.visibility_of_element_located)
        text = element.text
        if not text:
            # Try getting text via JavaScript if regular method returns empty
            text = driver.execute_script("return arguments[0].textContent;", element).strip()
        return text
    except Exception as e:
        logging.error(f"Error getting text from element: {e}")
        return default

def is_element_present(driver, locator_type, locator, timeout=5):
    """Check if an element is present on the page with timeout."""
    try:
        wait_for_element(driver, locator_type, locator, timeout, condition=EC.presence_of_element_located)
        return True
    except:
        return False

def wait_for_page_load(driver, timeout=30):
    """Wait for page to fully load."""
    try:
        old_page = driver.find_element(By.TAG_NAME, 'html')
        yield
        WebDriverWait(driver, timeout).until(EC.staleness_of(old_page))
        
        # Wait for document ready state
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
    except Exception as e:
        logging.warning(f"Error waiting for page load: {e}")
        
def find_download_button(driver):
    """Try different strategies to find and click the download button."""
    logging.info("Searching for download button")
    
    # Try different selectors for the download button
    possible_selectors = [
        # Exact selectors based on the HTML structure
        ("XPATH", "//a[contains(@href, '/api/v1/sqllab/export/')]"),
        ("XPATH", "//a[contains(@class, 'ant-btn')]"),
        ("XPATH", "//a[contains(@class, 'superset-button')]"),
        ("XPATH", "//a[.//span[contains(text(), 'Download to CSV')]]"),
        ("XPATH", "//a[contains(., 'Download')]"),
        ("CSS_SELECTOR", "a[href*='/api/v1/sqllab/export/']"),
        ("CSS_SELECTOR", "a.ant-btn"),
        ("CSS_SELECTOR", "a.superset-button")
    ]
    
    for selector_type, selector in possible_selectors:
        try:
            logging.info(f"Trying to find download button with: {selector}")
            if selector_type == "XPATH":
                buttons = driver.find_elements(By.XPATH, selector)
            else:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                
            if buttons:
                for button in buttons:
                    try:
                        # Check if this element might be our download button
                        button_text = button.text.strip()
                        button_html = button.get_attribute('outerHTML')
                        logging.info(f"Found potential button: {button_text}")
                        logging.info(f"HTML: {button_html[:100]}...")
                        
                        if 'Download' in button_text or 'download' in button_text.lower():
                            logging.info(f"Found download button with text: {button_text}")
                            return button
                        elif 'export' in button_html or '/api/v1/sqllab/export/' in button_html:
                            logging.info(f"Found download button with export in HTML")
                            return button
                    except Exception as e:
                        logging.warning(f"Error examining button: {e}")
                        continue
        except Exception as e:
            logging.warning(f"Error with selector {selector}: {e}")
            continue
    
    # If we get here, we haven't found the button with standard selectors
    # Try to extract from page source instead
    try:
        page_source = driver.page_source
        export_urls = re.findall(r'href=[\'"]?(/api/v1/sqllab/export/[^\'" >]+)', page_source)
        
        if export_urls:
            export_url = export_urls[0]
            logging.info(f"Found export URL in page source: {export_url}")
            
            # Try to find the element using this exact URL
            try:
                xpath = f"//a[contains(@href, '{export_url}')]"
                element = driver.find_element(By.XPATH, xpath)
                logging.info(f"Found download element using extracted URL")
                return element
            except NoSuchElementException:
                logging.warning(f"Could not find element with extracted URL: {export_url}")
                
                # Return the URL for direct navigation instead
                return export_url
    except Exception as e:
        logging.error(f"Error extracting export URL from page source: {e}")
    
    logging.warning("No download button found")
    return None

def download_query_results(driver, download_dir=None, timeout=30, stop_event=None):
    """Download the query results as CSV and return the path to the downloaded file."""
    logging.info("Attempting to download query results")
    
    if download_dir is None:
        download_dir = os.path.join(os.getcwd(), "downloads")
        # Ensure download directory exists
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
    
    logging.info(f"Download directory: {download_dir}")
    
    # Get the initial count of files in the download directory
    initial_files = set(os.listdir(download_dir))
    logging.info(f"Files in download directory before download: {len(initial_files)}")
    
    # Try to find and click the download button
    download_target = find_download_button(driver)
    
    if download_target:
        # Check if download_target is a WebElement or URL string
        if hasattr(download_target, 'tag_name'):
            # We have a WebElement, click it
            logging.info("Clicking download button...")
            try:
                download_target.click()
                logging.info("Clicked download button")
            except Exception as e:
                logging.error(f"Error clicking button: {e}")
                try:
                    # Try JavaScript click as fallback
                    driver.execute_script("arguments[0].click();", download_target)
                    logging.info("Used JavaScript to click download button")
                except Exception as js_e:
                    logging.error(f"JavaScript click also failed: {js_e}")
                    return None
        else:
            # Assume it's a URL string
            logging.info(f"Navigating directly to export URL: {download_target}")
            current_url = driver.current_url
            base_url = current_url.split('//', 1)[0] + '//' + current_url.split('//', 1)[1].split('/', 1)[0]
            full_url = f"{base_url}{download_target}"
            driver.get(full_url)
            time.sleep(2)  # Wait for download to start
            driver.get(current_url)  # Go back to results page
    else:
        logging.warning("Could not find download button.")
        return None
        
    # Wait for download to complete
    start_time = time.time()
    while time.time() - start_time < timeout:
        if stop_event and stop_event.is_set():
            logging.warning("Download cancelled by stop event")
            return None
            
        time.sleep(1)
        current_files = set(os.listdir(download_dir))
        new_files = current_files - initial_files
        
        # Check for new CSV files
        csv_files = [f for f in new_files if f.endswith('.csv')]
        if csv_files:
            # Sort by creation time, newest first
            csv_files.sort(key=lambda x: os.path.getctime(os.path.join(download_dir, x)), reverse=True)
            csv_path = os.path.join(download_dir, csv_files[0])
            
            # Make sure the file is fully downloaded (not a partial download)
            if not os.path.exists(csv_path + '.crdownload') and not os.path.exists(csv_path + '.part'):
                # Verify file size is stable (not still being written)
                size1 = os.path.getsize(csv_path)
                time.sleep(1)  # Wait a bit to check if size changes
                size2 = os.path.getsize(csv_path)
                
                if size1 == size2:  # File size is stable
                    logging.info(f"Download completed successfully: {csv_path}")
                    return csv_path
    
    logging.warning("Download timed out or failed")
    return None

def run_sql_query(driver, query_text, timeout=10, wait_results=120):
    """Execute a SQL query with enhanced reliability."""
    logging.info("Running SQL query")
    
    try:
        # Clear and fill the SQL editor
        try:
            # Try using JavaScript for ACE editor
            logging.info("Setting SQL query using JavaScript")
            driver.execute_script(
                f"ace.edit('ace-editor').setValue(`{query_text}`);",
            )
            logging.info("Successfully set SQL query text")
        except Exception as js_error:
            logging.warning(f"JavaScript approach failed: {js_error}, trying standard approach")
            
            # Fallback to standard approach
            try:
                editor = driver.find_element(By.ID, "ace-editor")
                editor.clear()
                editor.send_keys(query_text)
                logging.info("Set SQL query text using standard approach")
            except Exception as edit_e:
                logging.error(f"Could not set SQL text: {edit_e}")
                return False
        
        # Allow time to see the query in the editor
        time.sleep(2)
        
        # Take screenshot of editor before running
        driver.save_screenshot("sql_before_run.png")
        
        # Find and click the Run button with multiple selection strategies
        run_button = None
        
        # Try CSS selector
        try:
            run_button = driver.find_element(By.CSS_SELECTOR, "button.superset-button-primary")
            logging.info("Found run button by CSS selector")
        except:
            # Try XPath for "Run" button
            try:
                run_button = driver.find_element(By.XPATH, "//button[contains(., 'Run') or contains(@title, 'Run')]")
                logging.info("Found run button by XPath text")
            except:
                # Try button with play icon
                try:
                    run_button = driver.find_element(By.XPATH, "//button[.//span[contains(@class, 'icon-play')]]")
                    logging.info("Found run button by play icon")
                except:
                    logging.error("Could not find Run button")
                    return False
        
        # Click the run button
        try:
            run_button.click()
            logging.info("Clicked Run button")
        except Exception as click_e:
            logging.warning(f"Error clicking run button: {click_e}")
            try:
                # Try JavaScript click
                driver.execute_script("arguments[0].click();", run_button)
                logging.info("Clicked Run button using JavaScript")
            except Exception as js_e:
                logging.error(f"JavaScript click also failed: {js_e}")
                return False
        
        # Wait for results to load
        logging.info("Waiting for query results...")
        try:
            # Try multiple selectors for results
            result_selectors = [
                (By.CSS_SELECTOR, ".filterable-table-container"),
                (By.XPATH, "//div[contains(@class, 'ResultSet')]"),
                (By.XPATH, "//div[contains(@class, 'result-set')]"),
                (By.XPATH, "//table[contains(@class, 'result')]")
            ]
            
            results_found = False
            for selector_type, selector in result_selectors:
                try:
                    WebDriverWait(driver, wait_results).until(
                        EC.visibility_of_element_located((selector_type, selector))
                    )
                    logging.info(f"Query results found with selector: {selector}")
                    results_found = True
                    break
                except:
                    continue
            
            # Also check for "No result" message which is also a valid completion
            if not results_found:
                no_results_elements = driver.find_elements(By.XPATH, "//div[contains(., 'No results') or contains(., 'No data')]")
                if no_results_elements:
                    logging.info("Query completed with no results")
                    results_found = True
            
            if not results_found:
                logging.warning("Could not detect query results")
                return False
                
            # Take a screenshot of the results
            driver.save_screenshot("query_results.png")
            logging.info("Query executed successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error waiting for query results: {e}")
            driver.save_screenshot("query_error.png")
            return False
            
    except Exception as e:
        logging.error(f"Error running SQL query: {e}")
        driver.save_screenshot("sql_run_error.png")
        return False

# Function to set credentials for the selenium instance
def set_login_credentials(sel_instance, username=None, password=None):
    """Set login credentials for a Selenium instance."""
    # If credentials not provided, prompt for them
    if not username:
        username = input("Enter your username: ")
    
    if not password:
        import getpass
        password = getpass.getpass("Enter your password: ")
    
    # Set credentials on the selenium instance
    if hasattr(sel_instance, 'set_credentials'):
        sel_instance.set_credentials(username, password)
        logging.info(f"Credentials set for user: {username}")
        return True
    else:
        logging.error("The provided selenium instance doesn't support credential setting")
        return False
    



# class BackgroundTaskManager:
    # def __init__(self, logger=None):
    #     self.logger = logger or logging.getLogger('background_task')
    #     self.threads = []

    # def run(self, target_function, *args, **kwargs):
    #     def run_wrapper():
    #         try:
    #             self.logger.info(f"Running function: {target_function.__name__}")
    #             target_function(*args, **kwargs)
    #         except Exception as e:
    #             self.logger.error(f"Error in background thread: {e}")
    #         finally:
    #             self.logger.info(f"Task {target_function.__name__} completed")

    #     thread = threading.Thread(target=run_wrapper, daemon=True)
    #     thread.start()
    #     self.threads.append(thread)
    #     return thread

    # def wait_for_all(self):
    #     for thread in self.threads:
    #         thread.join()


class BackgroundTaskManager:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('background_task')
        self.threads = []

    def run(self, target_function, *args, **kwargs):
        def run_wrapper():
            try:
                self.logger.info(f"Running function: {target_function.__name__}")
                target_function(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"Error in background thread: {e}")
            finally:
                self.logger.info(f"Task {target_function.__name__} completed")

        thread = threading.Thread(target=run_wrapper, daemon=True)
        thread.start()
        self.threads.append(thread)
        return thread

    def wait_for_all(self):
        for thread in self.threads:
            thread.join()


class BackgroundSelenium_v2:
    def __init__(self, session_duration_minutes=60, log_file='selenium_background.log', cookies_path='cookies.pkl'):
        # Setup logging
        self.setup_logging(log_file)
        
        # Session management
        self.session_duration = timedelta(minutes=session_duration_minutes)
        self.session_start_time = None
        self.session_end_time = None
        self.is_running = False
        self.thread = None
        self.driver = None
        self.task_manager = BackgroundTaskManager(self.logger)
        self.stop_event = threading.Event()

        # Cookies configuration
        self.cookies_path = cookies_path
        self.cookies_loaded = False
        
        # Task queue
        self.task_queue = []
        self.queue_lock = threading.Lock()
        self.queue_thread = None
        self.queue_running = False
        
        # Register cleanup handlers
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Store session info
        self.session_info_file = 'selenium_session.json'

    def check_selenium_driver(self):
        """Check if the selenium driver is initialized and running"""
        return self.driver is not None and self.is_running

    def setup_logging(self, log_file):
        """Setup logging configuration"""
        self.logger = logging.getLogger('background_selenium')
        self.logger.setLevel(logging.INFO)
        
        # Check if handlers already exist before adding new ones
        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def initialize_driver(self):
        """Initialize the Selenium WebDriver with appropriate options"""
        self.logger.info("Initializing Chrome WebDriver...")
        
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Create a persistent Chrome profile directory
        user_data_dir = os.path.join(os.getcwd(), "chrome_profile")
        if not os.path.exists(user_data_dir):
            os.makedirs(user_data_dir)
        
        # Add the user data directory to Chrome options
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--profile-directory=Default")
        
        # Additional options to improve session persistence
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-site-isolation-trials")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Enable password saving and other preferences
        prefs = {
            "credentials_enable_service": True,
            "profile.password_manager_enabled": True,
            "autofill.profile_enabled": True,
            "download.default_directory": os.path.join(os.getcwd(), "downloads"),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        options.add_experimental_option("prefs", prefs)
        
        # Create driver
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )
            self.logger.info("Chrome WebDriver initialized successfully")
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
                """
            })
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            return False

    def start_thread(self, target_function, *args, **kwargs):
        """Start a function in a background thread with a WebDriver instance"""
        if self.is_running:
            self.logger.warning("Selenium is already running in background")
            return False
        if not self.initialize_driver():
            return False

        self.logger.info("Starting background task with Selenium")
        self.is_running = True
        self.task_manager.run(target_function, driver=self.driver, stop_event=self.stop_event, *args, **kwargs)
        return True

    def start(self, target_function, *args, **kwargs):
        """Start the Selenium process in a background thread with session management"""
        if self.is_running:
            self.logger.warning("Selenium is already running in background")
            return False
        
        # Initialize driver
        if not self.initialize_driver():
            return False
        
        # Set session timing
        self.session_start_time = datetime.now()
        self.session_end_time = self.session_start_time + self.session_duration
        
        # Save session info
        self.save_session_info()
        
        # Create and start the thread
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._run_with_expiration,
            args=(target_function, args, kwargs),
            daemon=True
        )
        self.thread.start()
        
        self.is_running = True
        self.logger.info(f"Selenium started in background thread. Session will expire at {self.session_end_time}")
        return True

    def _run_with_expiration(self, target_function, args, kwargs):
        """Run the target function with expiration check"""
        try:
            # Add driver to the kwargs
            kwargs['driver'] = self.driver
            kwargs['stop_event'] = self.stop_event
            
            # Run the target function
            self.logger.info(f"Running function: {target_function.__name__}")
            target_function(*args, **kwargs)
            
        except Exception as e:
            self.logger.error(f"Error in background thread: {e}")
        finally:
            self.cleanup_driver()
            self.is_running = False
            self.logger.info("Selenium background thread has completed")

    def check_expiration(self):
        """Check if the session has expired or if no session is set."""
        if not self.is_running:
            self.logger.info("No active session to check expiration.")
            return True

        if self.session_start_time is None or self.session_end_time is None:
            self.logger.info("Session timing not initialized. Please start a session first.")
            return False

        if datetime.now() >= self.session_end_time:
            self.logger.info("Session has expired.")
            self.stop()
            return True
        
        return False

    def extend_session(self, additional_minutes=60):
        """Extend the current session duration"""
        if not self.is_running:
            self.logger.warning("No active session to extend")
            return False
        
        self.session_end_time += timedelta(minutes=additional_minutes)
        self.logger.info(f"Session extended. New expiration time: {self.session_end_time}")
        
        # Update session info
        self.save_session_info()
        return True

    def stop(self):
        """Stop the Selenium process"""
        if not self.is_running:
            self.logger.warning("No active Selenium process to stop")
            return False
        
        self.logger.info("Stopping Selenium background process...")
        self.stop_event.set()
        
        # Wait for the thread to complete (with timeout)
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
        
        self.cleanup_driver()
        self.is_running = False
        self.logger.info("Selenium background process stopped")
        
        # Clean up session info
        if os.path.exists(self.session_info_file):
            try:
                os.remove(self.session_info_file)
                self.logger.info("Session info removed")
            except Exception as e:
                self.logger.error(f"Failed to remove session info: {e}")
        
        return True

    def cleanup_driver(self):
        """Clean up the WebDriver resources"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver resources released")
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {e}")
            finally:
                self.driver = None

    def cleanup(self):
        """Clean up resources when the program exits"""
        if self.is_running:
            self.stop()
        if self.queue_running:
            self.stop_queue()
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {sig}, cleaning up...")
        self.cleanup()
        # Allow normal signal processing to continue
        signal.default_int_handler(sig, frame)

    def save_cookies(self, path=None):
        """Save current browser cookies to a file"""
        if path is None:
            path = self.cookies_path
            
        try:
            if self.driver:
                cookies = self.driver.get_cookies()
                with open(path, 'wb') as file:
                    pickle.dump(cookies, file)
                self.logger.info(f"Cookies saved to {path}")
                return True
            else:
                self.logger.error("No driver available to save cookies")
                return False
        except Exception as e:
            self.logger.error(f"Error saving cookies: {e}")
            return False

    def load_cookies(self, path=None, url=None):
        """Load cookies from file into current browser session"""
        if path is None:
            path = self.cookies_path
            
        try:
            if not self.driver:
                self.logger.error("No driver available to load cookies")
                return False
                
            # Navigate to the domain first if provided
            if url:
                base_url = url.split('//', 1)[1].split('/', 1)[0]  # Extract domain
                self.driver.get(f"https://{base_url}")
            
            if not os.path.exists(path):
                self.logger.warning(f"Cookie file {path} not found")
                return False
                
            with open(path, 'rb') as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    # Some cookies cause issues, so try each one separately
                    try:
                        self.driver.add_cookie(cookie)
                    except Exception as e:
                        self.logger.warning(f"Error adding cookie: {e}")
            
            self.logger.info("Cookies loaded successfully")
            # Refresh to apply cookies
            self.driver.refresh()
            self.cookies_loaded = True
            return True
        except Exception as e:
            self.logger.error(f"Error loading cookies: {e}")
            self.cookies_loaded = False
            return False

    def get_to_site(self, url, driver=None, stop_event=None):
        """Navigate to a URL with advanced handling for login flows"""
        try:
            self.logger.info(f'Navigating to {url}...')
            driver = driver if driver is not None else self.driver
            
            # Try to load cookies first
            cookie_path = self.cookies_path
            cookies_loaded = self.load_cookies(path=cookie_path, url=url)
            # Store the status in the cookies_loaded property, not overwriting the path
            self.cookies_loaded = cookies_loaded
            
            # Navigate to the actual URL
            driver.get(url)
            self.logger.info(f'Initial navigation complete. Current URL: {driver.current_url}')
            
            # Check if we need to log in
            login_indicators = ["login", "signin", "auth", "account/auth"]
            needs_login = any(indicator in driver.current_url.lower() for indicator in login_indicators)
            
            # Also check for login form elements as a backup detection method
            try:
                login_elements = driver.find_elements(By.XPATH, "//input[@type='password']")
                if login_elements:
                    needs_login = True
            except:
                pass
                
            if needs_login:
                self.logger.info("Login page detected")
                
                # First attempt: Wait for the user to manually log in
                print("\n" + "="*50)
                print("Login required. Please log in manually in the browser window.")
                print("The automation will continue once you've logged in, cookies will be collected for automation.")
                print("="*50 + "\n")
                
                # Wait for login to complete - look for elements that would appear after successful login
                max_wait = 300  # 5 minutes max wait time
                wait_interval = 5  # Check every 5 seconds
                total_waited = 0
                
                while needs_login and total_waited < max_wait:
                    try:
                        # Sleep for the interval
                        time.sleep(wait_interval)
                        total_waited += wait_interval
                        
                        # Check URL again - if it changed from login page
                        current_url = driver.current_url
                        needs_login = any(indicator in current_url.lower() for indicator in login_indicators)
                        
                        if not needs_login:
                            self.logger.info("Login detected as complete based on URL change")
                            break
                            
                        # Try to find elements that would indicate successful login
                        success_elements = driver.find_elements(By.XPATH, "//button[contains(text(), 'Log out')]")
                        if success_elements:
                            self.logger.info("Login detected as complete based on page elements")
                            needs_login = False
                            break
                            
                        print(f"Waiting for login... ({total_waited} seconds elapsed)")
                            
                    except Exception as e:
                        self.logger.warning(f"Error while waiting for login: {e}")
                
                if needs_login:
                    self.logger.warning("Login timeout reached. Proceeding anyway.")
                else:
                    self.logger.info("Login completed successfully")
                    # Save the cookies for future use
                    self.save_cookies(path=cookie_path)
                    
                    # Reload the target URL
                    driver.get(url)
            
            # Wait for the page to load after successful login
            try:
                # Define elements that indicate the page is loaded - customize these for your application
                wait = WebDriverWait(driver, 20)
                
                # Wait for common page elements - adjust these selectors for your specific page
                for selector in [
                    (By.TAG_NAME, "body"),  # Basic element
                    (By.TAG_NAME, "header"),  # Common header element
                    (By.TAG_NAME, "main"),   # Common main content element
                ]:
                    try:
                        wait.until(EC.presence_of_element_located(selector))
                        self.logger.info(f"Found element {selector}")
                        break
                    except:
                        continue
                        
                self.logger.info("Page loaded successfully")
                
            except Exception as e:
                self.logger.warning(f"Timeout or error waiting for page elements: {e}")
                
            # Take a screenshot for debugging
            try:
                screenshot_path = os.path.join(os.getcwd(), "navigation_result.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Screenshot saved to {screenshot_path}")
            except Exception as e:
                self.logger.warning(f"Failed to take screenshot: {e}")
                
            # Print final status
            self.logger.info(f'Navigation complete. Final URL: {driver.current_url}')
            
        except Exception as e:
            self.logger.error(f'Error navigating to URL {url}: {e}')
 
    def wait_for_element(self, driver, locator_type, locator, timeout=10, condition=EC.element_to_be_clickable):
        """Utility function to wait for an element with proper error handling."""
        try:
            element = WebDriverWait(driver, timeout).until(
                condition((locator_type, locator))
            )
            return element
        except TimeoutException:
            self.logger.error(f"Timeout waiting for element: {locator}")
            # Take a screenshot to help debug
            screenshot_path = os.path.join(os.getcwd(), "element_wait_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.error(f"Screenshot saved to {screenshot_path}")
            self.logger.error(f"Current URL: {driver.current_url}")
            # Raise the exception after logging
            raise

    def click_element(self, driver, locator_type, locator, timeout=10, description=None):
        """Find and click an element."""
        element_desc = description or f"{locator_type}='{locator}'"
        self.logger.info(f"Clicking element: {element_desc}")
        
        try:
            element = self.wait_for_element(driver, locator_type, locator, timeout)
            element.click()
            self.logger.info(f"Successfully clicked element: {element_desc}")
            return True
        except Exception as e:
            self.logger.error(f"Error clicking element {element_desc}: {e}")
            screenshot_path = os.path.join(os.getcwd(), "click_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def fill_element(self, driver, locator_type, locator, text, timeout=10, description=None, clear_first=True):
        """Find and fill an input element with text."""
        element_desc = description or f"{locator_type}='{locator}'"
        self.logger.info(f"Filling element {element_desc} with text: {text}")
        
        try:
            element = self.wait_for_element(driver, locator_type, locator, timeout, condition=EC.visibility_of_element_located)
            
            if clear_first:
                element.clear()
                
            element.send_keys(text)
            self.logger.info(f"Successfully filled element: {element_desc}")
            return True
        except Exception as e:
            self.logger.error(f"Error filling element {element_desc}: {e}")
            screenshot_path = os.path.join(os.getcwd(), "fill_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def select_dropdown_item(self, driver, dropdown_locator, dropdown_type, item_locator, item_type, 
                            dropdown_description=None, item_description=None, wait_time=2, timeout=10):
        """Select an item from a dropdown menu.
        
        Args:
            driver: WebDriver instance
            dropdown_locator: Locator string for the dropdown element
            dropdown_type: By.* type for the dropdown element
            item_locator: Locator string for the item to select
            item_type: By.* type for the item element
            dropdown_description: Human-readable description of the dropdown
            item_description: Human-readable description of the item
            wait_time: Time to wait after clicking dropdown before selecting item
            timeout: Maximum time to wait for elements
        """
        dropdown_desc = dropdown_description or f"{dropdown_type}='{dropdown_locator}'"
        item_desc = item_description or f"{item_type}='{item_locator}'"
        
        self.logger.info(f"Selecting {item_desc} from dropdown {dropdown_desc}")
        
        try:
            # Click the dropdown to open it
            dropdown_clicked = self.click_element(
                driver, 
                dropdown_type, 
                dropdown_locator,
                timeout=timeout,
                description=dropdown_desc
            )
            
            if not dropdown_clicked:
                self.logger.error(f"Failed to open dropdown {dropdown_desc}")
                return False
                
            # Wait for dropdown to expand
            time.sleep(wait_time)
            
            # Click the desired item
            item_clicked = self.click_element(
                driver,
                item_type,
                item_locator,
                timeout=timeout,
                description=item_desc
            )
            
            if not item_clicked:
                self.logger.error(f"Failed to select item {item_desc} from dropdown")
                return False
                
            self.logger.info(f"Successfully selected {item_desc} from dropdown {dropdown_desc}")
            return True
        except Exception as e:
            self.logger.error(f"Error during dropdown selection: {e}")
            screenshot_path = os.path.join(os.getcwd(), "dropdown_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def select_database(self, driver, database_name, timeout=10):
        """Select a database from the dropdown."""
        self.logger.info(f"Selecting database: {database_name}")
        
        return self.select_dropdown_item(
            driver=driver,
            dropdown_locator="//span[contains(@class, 'ant-select-selection-item')]/div[contains(@class, 'css-1jje2m4')]",
            dropdown_type=By.XPATH,
            item_locator=f"//div[contains(@class, 'ant-select-item-option')]//span[@title='{database_name}']",
            item_type=By.XPATH,
            dropdown_description="Database dropdown",
            item_description=f"'{database_name}' database",
            timeout=timeout
        )

    def select_schema(self, driver, schema_name, timeout=10):
        """Select a schema from the dropdown."""
        self.logger.info(f"Selecting schema: {schema_name}")
        
        return self.select_dropdown_item(
            driver=driver,
            dropdown_locator=f"//span[contains(@class, 'ant-select-selection-item') and @title='{schema_name}']",
            dropdown_type=By.XPATH,
            item_locator=f"//div[@title='{schema_name}']",
            item_type=By.XPATH,
            dropdown_description="Schema dropdown",
            item_description=f"'{schema_name}' schema",
            timeout=timeout
        )

    def enter_query_into_editor(self, driver, query_text, timeout=10):
        try:
            # Try using JavaScript for ACE editor
            self.logger.info("Setting SQL query using JavaScript")
            driver.execute_script(
                f"ace.edit('ace-editor').setValue(`{query_text}`);",
            )
            self.logger.info("Successfully set SQL query text")
        except Exception as js_error:
            self.logger.warning(f"JavaScript approach failed: {js_error}, trying standard approach")
            # Fallback to standard approach
            editor_filled = self.fill_element(
                driver,
                By.ID,
                "ace-editor",
                query_text,
                timeout=timeout,
                description="SQL editor"
            )
            if not editor_filled:
                self.logger.error("Failed to set SQL query text")
                return False

    def run_sql_query(self, driver, query_text, timeout=10):
        """Enter and run a SQL query."""
        self.logger.info("Running SQL query")
        
        try:
            # Clear and fill the SQL editor
            # Note: For complex editors like ACE, we might need a custom approach
            try:
                # Try using JavaScript for ACE editor
                self.logger.info("Setting SQL query using JavaScript")
                driver.execute_script(
                    f"ace.edit('ace-editor').setValue(`{query_text}`);",
                )
                self.logger.info("Successfully set SQL query text")
            except Exception as js_error:
                self.logger.warning(f"JavaScript approach failed: {js_error}, trying standard approach")
                # Fallback to standard approach
                editor_filled = self.fill_element(
                    driver,
                    By.ID,
                    "ace-editor",
                    query_text,
                    timeout=timeout,
                    description="SQL editor"
                )
                if not editor_filled:
                    self.logger.error("Failed to set SQL query text")
                    return False
            
            # Allow time to see the query in the editor
            time.sleep(2)
            
            # Find and click the Run button
            run_success = self.click_element(
                driver,
                By.CSS_SELECTOR,
                "button.superset-button-primary",
                timeout=timeout,
                description="Run SQL button"
            )
            
            if not run_success:
                self.logger.error("Failed to click Run SQL button")
                return False
            
            # Wait for results to load
            self.logger.info("Waiting for query results...")
            try:
                results_element = self.wait_for_element(
                    driver,
                    By.CSS_SELECTOR,
                    ".filterable-table-container",
                    timeout=120,  # Longer timeout for query execution
                    condition=EC.visibility_of_element_located
                )
                self.logger.info("Query executed successfully, results displayed")
                
                # Take a screenshot of the results
                screenshot_path = os.path.join(os.getcwd(), "query_results.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Results screenshot saved to {screenshot_path}")
                
                return True
            except Exception as e:
                self.logger.error(f"Error waiting for query results: {e}")
                screenshot_path = os.path.join(os.getcwd(), "query_error.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Error screenshot saved to {screenshot_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error running SQL query: {e}")
            screenshot_path = os.path.join(os.getcwd(), "sql_run_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def setup_sql_environment(self, driver, database_name, schema_name, continue_on_error=False):
        """Set up the SQL environment by selecting database and schema."""
        self.logger.info(f"Setting up SQL environment: database={database_name}, schema={schema_name}")
        
        # Select database
        database_selected = self.select_database(driver, database_name)
        if not database_selected and not continue_on_error:
            manual_continue = input("Could not select database. Do you want to continue? (y/n): ")
            if manual_continue.lower() != 'y':
                self.logger.info("User chose to abort after database selection failure")
                return False
        
        # Select schema
        schema_selected = self.select_schema(driver, schema_name)
        if not schema_selected and not continue_on_error:
            manual_continue = input("Could not select schema. Do you want to continue? (y/n): ")
            if manual_continue.lower() != 'y':
                self.logger.info("User chose to abort after schema selection failure")
                return False
        
        self.logger.info("SQL environment setup complete")
        return database_selected and schema_selected

    def sql_workflow(self, driver=None, stop_event=None, url=None, query=None):
        """Complete SQL workflow: navigate, setup environment, run query."""
        try:
            # Navigate to SQL page
            self.get_to_site(url, driver, stop_event)
            
            # Set up SQL environment
            success = self.setup_sql_environment(
                driver=driver, 
                database_name="Airdrop campaign", 
                schema_name="public"
            )
            
            if not success:
                self.logger.warning("SQL environment setup failed or was aborted")
                return
            
            # If a query was provided, run it
            if query:
                query_success = self.run_sql_query(driver, query)
                if query_success:
                    self.logger.info("SQL query executed successfully")
                else:
                    self.logger.warning("SQL query execution failed")
            
        except Exception as e:
            self.logger.error(f"Error in SQL workflow: {e}")

    # Task Queue Methods
    def start_queue_processor(self):
        """Start a background thread that processes tasks from the queue with session management"""
        if self.queue_thread and self.queue_thread.is_alive():
            self.logger.warning("Task queue is already running")
            return False
            
        if not self.initialize_driver():
            self.logger.error("Failed to initialize driver for task queue")
            return False
            
        # Set up session timing
        self.session_start_time = datetime.now()
        self.session_end_time = self.session_start_time + self.session_duration
        
        # Save session info
        self.save_session_info()
        
        self.queue_running = True
        self.is_running = True
        self.queue_thread = threading.Thread(
            target=self._process_queue_with_expiration,
            daemon=True
        )
        self.queue_thread.start()
        self.logger.info(f"Task queue processor started. Session will expire at {self.session_end_time}")
        return True
        
    def _process_queue_with_expiration(self):
        """Process tasks from the queue until stopped or session expires"""
        try:
            while self.queue_running and not self.stop_event.is_set():
                # Check for session expiration
                if datetime.now() >= self.session_end_time:
                    self.logger.info("Session has expired.")
                    break
                    
                task = None
                with self.queue_lock:
                    if self.task_queue:
                        task = self.task_queue.pop(0)
                        
                if task:
                    func, args, kwargs = task
                    try:
                        # Add standard parameters
                        kwargs['driver'] = self.driver
                        kwargs['stop_event'] = self.stop_event
                        self.logger.info(f"Executing task: {func.__name__}")
                        func(*args, **kwargs)
                    except Exception as e:
                        self.logger.error(f"Error executing task {func.__name__}: {e}")
                        # Take a screenshot for troubleshooting
                        if self.driver:
                            screenshot_path = os.path.join(os.getcwd(), f"task_error_{func.__name__}.png")
                            try:
                                self.driver.save_screenshot(screenshot_path)
                                self.logger.info(f"Error screenshot saved to {screenshot_path}")
                            except:
                                pass
                else:
                    # No tasks, sleep briefly
                    time.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Error in queue processor: {e}")
        finally:
            # Clean up resources
            self.logger.info("Queue processor stopping, cleaning up resources")
            self.cleanup_driver()
            self.is_running = False
            self.queue_running = False
            
            # Clean up session info
            if os.path.exists(self.session_info_file):
                try:
                    os.remove(self.session_info_file)
                    self.logger.info("Session info removed")
                except Exception as e:
                    self.logger.error(f"Failed to remove session info: {e}")
                
    def add_task(self, func, *args, **kwargs):
        """Add a task to the queue for execution"""
        with self.queue_lock:
            self.task_queue.append((func, args, kwargs))
        self.logger.info(f"Task added to queue: {func.__name__}")
        return True
        
    def extend_queue_session(self, additional_minutes=60):
        """Extend the current session duration for the task queue"""
        if not self.queue_running:
            self.logger.warning("No active queue session to extend")
            return False
        
        self.session_end_time += timedelta(minutes=additional_minutes)
        self.logger.info(f"Session extended. New expiration time: {self.session_end_time}")
        
        # Update session info
        self.save_session_info()
        return True
        
    def stop_queue(self):
        """Stop the task queue processor"""
        if not self.queue_running:
            self.logger.warning("Task queue is not running")
            return False
            
        self.logger.info("Stopping task queue processor...")
        self.queue_running = False
        self.stop_event.set()
        
        if self.queue_thread and self.queue_thread.is_alive():
            self.queue_thread.join(timeout=10)
            
        self.logger.info("Task queue processor stopped")
        return True
        
    def wait_for_queue_empty(self, timeout=None):
        """Wait until the task queue is empty or timeout is reached"""
        start_time = time.time()
        while self.queue_running:
            with self.queue_lock:
                if not self.task_queue:
                    return True
            
            if timeout and (time.time() - start_time > timeout):
                self.logger.warning(f"Timeout reached waiting for queue to empty ({timeout}s)")
                return False
                
            time.sleep(0.5)
            
        return not self.task_queue
        
    def save_session_info(self):
        """Save session information to a file"""
        try:
            session_info = {
                "session_start": self.session_start_time.isoformat() if self.session_start_time else None,
                "session_end": self.session_end_time.isoformat() if self.session_end_time else None,
                "is_running": self.is_running,
                "queue_running": self.queue_running,
                "tasks_count": len(self.task_queue)
            }
            
            with open(self.session_info_file, 'w') as f:
                import json
                json.dump(session_info, f)
                
            self.logger.info("Session info saved")
        except Exception as e:
            self.logger.error(f"Error saving session info: {e}")
            
    def load_session_info(self):
        """Load session information from a file"""
        if not os.path.exists(self.session_info_file):
            return False
        
        try:
            with open(self.session_info_file, 'r') as f:
                import json
                session_info = json.load(f)
            
            self.session_start_time = datetime.fromisoformat(session_info.get("session_start")) if session_info.get("session_start") else None
            self.session_end_time = datetime.fromisoformat(session_info.get("session_end")) if session_info.get("session_end") else None
            
            # Check if the session is still valid
            if self.session_end_time and datetime.now() < self.session_end_time:
                self.logger.info(f"Found valid session, expires at {self.session_end_time}")
                return True
            else:
                self.logger.info("Found expired session")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load session info: {e}")
            return False

class BackgroundSelenium:
    def __init__(self, session_duration_minutes=60, log_file='selenium_background.log', cookies_path=None):
        # Setup logging
        self.setup_logging(log_file)
        
        # Session management
        self.session_duration = timedelta(minutes=session_duration_minutes)
        self.session_start_time = None
        self.session_end_time = None
        self.is_running = False
        self.thread = None
        self.driver = None
        self.task_manager = BackgroundTaskManager(self.logger)
        self.stop_event = threading.Event()

        # Cookies configuration
        self.cookies_path = cookies_path
        self.cookies_loaded = False
        
        # Register cleanup handlers
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Store session info
        self.session_info_file = 'selenium_session.json'

    def check_selenium_driver(self):
        pass

    def setup_logging(self, log_file):
        """Setup logging configuration"""
        self.logger = logging.getLogger('background_selenium')
        self.logger.setLevel(logging.INFO)
        
        # Check if handlers already exist before adding new ones
        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def initialize_driver(self):
        """Initialize the Selenium WebDriver with appropriate options"""
        self.logger.info("Initializing Chrome WebDriver...")
        
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Create a persistent Chrome profile directory
        user_data_dir = os.path.join(os.getcwd(), "chrome_profile")
        if not os.path.exists(user_data_dir):
            os.makedirs(user_data_dir)
        
        # Add the user data directory to Chrome options
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--profile-directory=Default")
        
        # Additional options to improve session persistence
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-site-isolation-trials")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Enable password saving and other preferences
        prefs = {
            "credentials_enable_service": True,
            "profile.password_manager_enabled": True,
            "autofill.profile_enabled": True,
            "download.default_directory": os.path.join(os.getcwd(), "downloads"),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        options.add_experimental_option("prefs", prefs)
        
        # Create driver
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )
            self.logger.info("Chrome WebDriver initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            return False


    def start_thread(self, target_function, *args, **kwargs):
        if self.is_running:
            self.logger.warning("Selenium is already running in background")
            return False
        if not self.initialize_driver():
            return False

        self.logger.info("Starting background task with Selenium")
        self.is_running = True
        self.task_manager.run(target_function, driver=self.driver, stop_event=self.stop_event, *args, **kwargs)
        return True

    def start(self, target_function, *args, **kwargs):
        """Start the Selenium process in a background thread"""
        if self.is_running:
            self.logger.warning("Selenium is already running in background")
            return False
        
        # Initialize driver
        if not self.initialize_driver():
            return False
        
        # Set session timing
        self.session_start_time = datetime.now()
        self.session_end_time = self.session_start_time + self.session_duration
        
        # Save session info
        self.save_session_info()
        
        # Create and start the thread
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._run_with_expiration,
            args=(target_function, args, kwargs),
            daemon=True
        )
        self.thread.start()
        
        self.is_running = True
        self.logger.info(f"Selenium started in background thread. Session will expire at {self.session_end_time}")
        return True

    def _run_with_expiration(self, target_function, args, kwargs):
        """Run the target function with expiration check"""
        try:
            # Add driver to the kwargs
            kwargs['driver'] = self.driver
            kwargs['stop_event'] = self.stop_event
            
            # Run the target function
            self.logger.info(f"Running function: {target_function.__name__}")
            target_function(*args, **kwargs)
            
        except Exception as e:
            self.logger.error(f"Error in background thread: {e}")
        finally:
            self.cleanup_driver()
            self.is_running = False
            self.logger.info("Selenium background thread has completed")

    def check_expiration(self):
        """Check if the session has expired or if no session is set."""
        if not self.is_running:
            self.logger.info("No active session to check expiration.")
            return True

        if self.session_start_time is None or self.session_end_time is None:
            self.logger.info("Session timing not initialized. Please start a session first.")
            return False

        if datetime.now() >= self.session_end_time:
            self.logger.info("Session has expired.")
            self.stop()
            return True
        
        return False

    def extend_session(self, additional_minutes=60):
        """Extend the current session duration"""
        if not self.is_running:
            self.logger.warning("No active session to extend")
            return False
        
        self.session_end_time += timedelta(minutes=additional_minutes)
        self.logger.info(f"Session extended. New expiration time: {self.session_end_time}")
        
        # Update session info
        self.save_session_info()
        return True

    def stop(self):
        """Stop the Selenium process"""
        if not self.is_running:
            self.logger.warning("No active Selenium process to stop")
            return False
        
        self.logger.info("Stopping Selenium background process...")
        self.stop_event.set()
        
        # Wait for the thread to complete (with timeout)
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
        
        self.cleanup_driver()
        self.is_running = False
        self.logger.info("Selenium background process stopped")
        
        # Clean up session info
        if os.path.exists(self.session_info_file):
            try:
                os.remove(self.session_info_file)
                self.logger.info("Session info removed")
            except Exception as e:
                self.logger.error(f"Failed to remove session info: {e}")
        
        return True

    def cleanup_driver(self):
        """Clean up the WebDriver resources"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver resources released")
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {e}")
            finally:
                self.driver = None

    def cleanup(self):
        """Clean up resources when the program exits"""
        if self.is_running:
            self.stop()
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {sig}, cleaning up...")
        self.cleanup()
        # Allow normal signal processing to continue
        signal.default_int_handler(sig, frame)

    def save_cookies(self, path='cookies.pkl'):
        """Save current browser cookies to a file"""
        try:
            if self.driver:
                cookies = self.driver.get_cookies()
                with open(path, 'wb') as file:
                    pickle.dump(cookies, file)
                self.logger.info(f"Cookies saved to {path}")
                return True
            else:
                self.logger.error("No driver available to save cookies")
                return False
        except Exception as e:
            self.logger.error(f"Error saving cookies: {e}")
            return False

    def load_cookies(self, path='cookies.pkl', url=None):
        """Load cookies from file into current browser session"""
        try:
            if not self.driver:
                self.logger.error("No driver available to load cookies")
                return False
                
            # Navigate to the domain first if provided
            if url:
                base_url = url.split('//', 1)[1].split('/', 1)[0]  # Extract domain
                self.driver.get(f"https://{base_url}")
            
            if not os.path.exists(path):
                self.logger.warning(f"Cookie file {path} not found")
                return False
                
            with open(path, 'rb') as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    # Some cookies cause issues, so try each one separately
                    try:
                        self.driver.add_cookie(cookie)
                    except Exception as e:
                        self.logger.warning(f"Error adding cookie: {e}")
            
            self.logger.info("Cookies loaded successfully")
            # Refresh to apply cookies
            self.driver.refresh()
            self.cookies_loaded = True
            self.cookies_path = path
            return True, path
        except Exception as e:
            self.logger.error(f"Error loading cookies: {e}")
            self.cookies_loaded = False
            return False, path

    def get_to_site(self, url, driver=None, stop_event=None):
        """Navigate to a URL with advanced handling for login flows"""
        try:
            self.logger.info(f'Navigating to {url}...')
            driver = driver if driver is not None else self.driver
            
            # Try to load cookies first
            # cookie_path = self.cookies_path
            cookies_loaded, cookie_path = self.load_cookies(url=url)
            self.cookies_loaded = cookies_loaded
            
            # Navigate to the actual URL
            driver.get(url)
            self.logger.info(f'Initial navigation complete. Current URL: {driver.current_url}')
            
            # Check if we need to log in
            login_indicators = ["login", "signin", "auth", "account/auth"]
            needs_login = any(indicator in driver.current_url.lower() for indicator in login_indicators)
            
            # Also check for login form elements as a backup detection method
            try:
                login_elements = driver.find_elements(By.XPATH, "//input[@type='password']")
                if login_elements:
                    needs_login = True
            except:
                pass
                
            if needs_login:
                self.logger.info("Login page detected")
                
                # First attempt: Wait for the user to manually log in
                print("\n" + "="*50)
                print("Login required. Please log in manually in the browser window.")
                print("The automation will continue once you've logged in, cookies will be collected for automation.")
                print("="*50 + "\n")
                
                # Wait for login to complete - look for elements that would appear after successful login
                max_wait = 300  # 5 minutes max wait time
                wait_interval = 5  # Check every 5 seconds
                total_waited = 0
                
                while needs_login and total_waited < max_wait:
                    try:
                        # Sleep for the interval
                        time.sleep(wait_interval)
                        total_waited += wait_interval
                        
                        # Check URL again - if it changed from login page
                        current_url = driver.current_url
                        needs_login = any(indicator in current_url.lower() for indicator in login_indicators)
                        
                        if not needs_login:
                            self.logger.info("Login detected as complete based on URL change")
                            break
                            
                        # Try to find elements that would indicate successful login
                        success_elements = driver.find_elements(By.XPATH, "//button[contains(text(), 'Log out')]")
                        if success_elements:
                            self.logger.info("Login detected as complete based on page elements")
                            needs_login = False
                            break
                            
                        print(f"Waiting for login... ({total_waited} seconds elapsed)")
                            
                    except Exception as e:
                        self.logger.warning(f"Error while waiting for login: {e}")
                
                if needs_login:
                    self.logger.warning("Login timeout reached. Proceeding anyway.")
                else:
                    self.logger.info("Login completed successfully")
                    # Save the cookies for future use
                    self.save_cookies(path=cookie_path)
                    
                    # Reload the target URL
                    driver.get(url)
            
            # Wait for the page to load after successful login
            try:
                # Define elements that indicate the page is loaded - customize these for your application
                wait = WebDriverWait(driver, 20)
                
                # Wait for common page elements - adjust these selectors for your specific page
                for selector in [
                    (By.TAG_NAME, "body"),  # Basic element
                    (By.TAG_NAME, "header"),  # Common header element
                    (By.TAG_NAME, "main"),   # Common main content element
                ]:
                    try:
                        wait.until(EC.presence_of_element_located(selector))
                        self.logger.info(f"Found element {selector}")
                        break
                    except:
                        continue
                        
                self.logger.info("Page loaded successfully")
                
            except Exception as e:
                self.logger.warning(f"Timeout or error waiting for page elements: {e}")
                
            # Take a screenshot for debugging
            try:
                screenshot_path = os.path.join(os.getcwd(), "navigation_result.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Screenshot saved to {screenshot_path}")
            except Exception as e:
                self.logger.warning(f"Failed to take screenshot: {e}")
                
            # Print final status
            self.logger.info(f'Navigation complete. Final URL: {driver.current_url}')
            
        except Exception as e:
            self.logger.error(f'Error navigating to URL {url}: {e}')
            
        return
 
    def wait_for_element(self, driver, locator_type, locator, timeout=10, condition=EC.element_to_be_clickable):
        """Utility function to wait for an element with proper error handling."""
        try:
            element = WebDriverWait(driver, timeout).until(
                condition((locator_type, locator))
            )
            return element
        except TimeoutException:
            self.logger.error(f"Timeout waiting for element: {locator}")
            # Take a screenshot to help debug
            screenshot_path = os.path.join(os.getcwd(), "element_wait_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.error(f"Screenshot saved to {screenshot_path}")
            self.logger.error(f"Current URL: {driver.current_url}")
            # Raise the exception after logging
            raise

    def click_element(self, driver, locator_type, locator, timeout=10, description=None):
        """Find and click an element."""
        element_desc = description or f"{locator_type}='{locator}'"
        self.logger.info(f"Clicking element: {element_desc}")
        
        try:
            element = self.wait_for_element(driver, locator_type, locator, timeout)
            element.click()
            self.logger.info(f"Successfully clicked element: {element_desc}")
            return True
        except Exception as e:
            self.logger.error(f"Error clicking element {element_desc}: {e}")
            screenshot_path = os.path.join(os.getcwd(), "click_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def fill_element(self, driver, locator_type, locator, text, timeout=10, description=None, clear_first=True):
        """Find and fill an input element with text."""
        element_desc = description or f"{locator_type}='{locator}'"
        self.logger.info(f"Filling element {element_desc} with text: {text}")
        
        try:
            element = self.wait_for_element(driver, locator_type, locator, timeout, condition=EC.visibility_of_element_located)
            
            if clear_first:
                element.clear()
                
            element.send_keys(text)
            self.logger.info(f"Successfully filled element: {element_desc}")
            return True
        except Exception as e:
            self.logger.error(f"Error filling element {element_desc}: {e}")
            screenshot_path = os.path.join(os.getcwd(), "fill_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def select_dropdown_item(self, driver, dropdown_locator, dropdown_type, item_locator, item_type, 
                            dropdown_description=None, item_description=None, wait_time=2, timeout=10):
        """Select an item from a dropdown menu.
        
        Args:
            driver: WebDriver instance
            dropdown_locator: Locator string for the dropdown element
            dropdown_type: By.* type for the dropdown element
            item_locator: Locator string for the item to select
            item_type: By.* type for the item element
            dropdown_description: Human-readable description of the dropdown
            item_description: Human-readable description of the item
            wait_time: Time to wait after clicking dropdown before selecting item
            timeout: Maximum time to wait for elements
        """
        dropdown_desc = dropdown_description or f"{dropdown_type}='{dropdown_locator}'"
        item_desc = item_description or f"{item_type}='{item_locator}'"
        
        self.logger.info(f"Selecting {item_desc} from dropdown {dropdown_desc}")
        
        try:
            # Click the dropdown to open it
            dropdown_clicked = self.click_element(
                driver, 
                dropdown_type, 
                dropdown_locator,
                timeout=timeout,
                description=dropdown_desc
            )
            
            if not dropdown_clicked:
                self.logger.error(f"Failed to open dropdown {dropdown_desc}")
                return False
                
            # Wait for dropdown to expand
            time.sleep(wait_time)
            
            # Click the desired item
            item_clicked = self.click_element(
                driver,
                item_type,
                item_locator,
                timeout=timeout,
                description=item_desc
            )
            
            if not item_clicked:
                self.logger.error(f"Failed to select item {item_desc} from dropdown")
                return False
                
            self.logger.info(f"Successfully selected {item_desc} from dropdown {dropdown_desc}")
            return True
        except Exception as e:
            self.logger.error(f"Error during dropdown selection: {e}")
            screenshot_path = os.path.join(os.getcwd(), "dropdown_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False
        

    def select_database(self, driver, database_name, timeout=10):
        """Select a database from the dropdown."""
        self.logger.info(f"Selecting database: {database_name}")
        
        return self.select_dropdown_item(
            driver=driver,
            dropdown_locator="//span[contains(@class, 'ant-select-selection-item')]/div[contains(@class, 'css-1jje2m4')]",
            dropdown_type=By.XPATH,
            item_locator=f"//div[contains(@class, 'ant-select-item-option')]//span[@title='{database_name}']",
            item_type=By.XPATH,
            dropdown_description="Database dropdown",
            item_description=f"'{database_name}' database",
            timeout=timeout
        )

    def select_schema(self, driver, schema_name, timeout=10):
        """Select a schema from the dropdown."""
        self.logger.info(f"Selecting schema: {schema_name}")
        
        return self.select_dropdown_item(
            driver=driver,
            dropdown_locator=f"//span[contains(@class, 'ant-select-selection-item') and @title='{schema_name}']",
            dropdown_type=By.XPATH,
            item_locator=f"//div[@title='{schema_name}']",
            item_type=By.XPATH,
            dropdown_description="Schema dropdown",
            item_description=f"'{schema_name}' schema",
            timeout=timeout
        )

    def run_sql_query(self, driver, query_text, timeout=10):
        """Enter and run a SQL query."""
        self.logger.info("Running SQL query")
        
        try:
            # Clear and fill the SQL editor
            # Note: For complex editors like ACE, we might need a custom approach
            try:
                # Try using JavaScript for ACE editor
                self.logger.info("Setting SQL query using JavaScript")
                driver.execute_script(
                    f"ace.edit('ace-editor').setValue(`{query_text}`);",
                )
                self.logger.info("Successfully set SQL query text")
            except Exception as js_error:
                self.logger.warning(f"JavaScript approach failed: {js_error}, trying standard approach")
                # Fallback to standard approach
                editor_filled = self.fill_element(
                    driver,
                    By.ID,
                    "ace-editor",
                    query_text,
                    timeout=timeout,
                    description="SQL editor"
                )
                if not editor_filled:
                    self.logger.error("Failed to set SQL query text")
                    return False
            
            # Allow time to see the query in the editor
            time.sleep(2)
            
            # Find and click the Run button
            run_success = self.click_element(
                driver,
                By.CSS_SELECTOR,
                "button.superset-button-primary",
                timeout=timeout,
                description="Run SQL button"
            )
            
            if not run_success:
                self.logger.error("Failed to click Run SQL button")
                return False
            
            # Wait for results to load
            self.logger.info("Waiting for query results...")
            try:
                results_element = self.wait_for_element(
                    driver,
                    By.CSS_SELECTOR,
                    ".filterable-table-container",
                    timeout=120,  # Longer timeout for query execution
                    condition=EC.visibility_of_element_located
                )
                self.logger.info("Query executed successfully, results displayed")
                
                # Take a screenshot of the results
                screenshot_path = os.path.join(os.getcwd(), "query_results.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Results screenshot saved to {screenshot_path}")
                
                return True
            except Exception as e:
                self.logger.error(f"Error waiting for query results: {e}")
                screenshot_path = os.path.join(os.getcwd(), "query_error.png")
                driver.save_screenshot(screenshot_path)
                self.logger.info(f"Error screenshot saved to {screenshot_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error running SQL query: {e}")
            screenshot_path = os.path.join(os.getcwd(), "sql_run_error.png")
            driver.save_screenshot(screenshot_path)
            self.logger.info(f"Screenshot saved to {screenshot_path}")
            return False

    def find_download_button(self, driver):
        """Try different strategies to find and click the download button."""
        self.logger.info("Searching for download button")
        
        # Try different selectors for the download button
        possible_selectors = [
            # Exact selectors based on the HTML structure
            ("XPATH", "//a[contains(@href, '/api/v1/sqllab/export/')]"),
            ("XPATH", "//a[contains(@class, 'ant-btn')]"),
            ("XPATH", "//a[contains(@class, 'superset-button')]"),
            ("XPATH", "//a[.//span[contains(text(), 'Download to CSV')]]"),
            ("XPATH", "//a[contains(., 'Download')]"),
            ("CSS_SELECTOR", "a[href*='/api/v1/sqllab/export/']"),
            ("CSS_SELECTOR", "a.ant-btn"),
            ("CSS_SELECTOR", "a.superset-button")
        ]
        
        for selector_type, selector in possible_selectors:
            try:
                self.logger.info(f"Trying to find download button with: {selector}")
                if selector_type == "XPATH":
                    buttons = driver.find_elements(By.XPATH, selector)
                else:
                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                if buttons:
                    for button in buttons:
                        try:
                            # Check if this element might be our download button
                            button_text = button.text.strip()
                            button_html = button.get_attribute('outerHTML')
                            self.logger.info(f"Found potential button: {button_text}")
                            self.logger.info(f"HTML: {button_html[:100]}...")
                            
                            if 'Download' in button_text or 'download' in button_text.lower():
                                self.logger.info(f"Found download button with text: {button_text}")
                                return button
                            elif 'export' in button_html or '/api/v1/sqllab/export/' in button_html:
                                self.logger.info(f"Found download button with export in HTML")
                                return button
                        except Exception as e:
                            self.logger.warning(f"Error examining button: {e}")
                            continue
            except Exception as e:
                self.logger.warning(f"Error with selector {selector}: {e}")
                continue
        
        # If we get here, we haven't found the button with standard selectors
        # Try to extract from page source instead
        try:
            page_source = driver.page_source
            export_urls = re.findall(r'href=[\'"]?(/api/v1/sqllab/export/[^\'" >]+)', page_source)
            
            if export_urls:
                export_url = export_urls[0]
                self.logger.info(f"Found export URL in page source: {export_url}")
                
                # Try to find the element using this exact URL
                try:
                    xpath = f"//a[contains(@href, '{export_url}')]"
                    element = driver.find_element(By.XPATH, xpath)
                    self.logger.info(f"Found download element using extracted URL")
                    return element
                except NoSuchElementException:
                    self.logger.warning(f"Could not find element with extracted URL: {export_url}")
                    
                    # Return the URL for direct navigation instead
                    return export_url
        except Exception as e:
            self.logger.error(f"Error extracting export URL from page source: {e}")
        
        self.logger.warning("No download button found")
        return None

    def download_query_results(self, driver, download_dir=None, timeout=30, stop_event=None):
        """Download the query results as CSV and return the path to the downloaded file.
        
        Args:
            driver: WebDriver instance
            download_dir: Directory where the download will be saved
            timeout: Maximum time to wait for download in seconds
            stop_event: Event to check for cancellation
            
        Returns:
            Path to the downloaded file or None if download failed
        """
        if download_dir is None:
            download_dir = os.path.join(os.getcwd(), "downloads")
            # Ensure download directory exists
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
        
        self.logger.info(f"Attempting to download query results to {download_dir}")
        
        # Get the initial count of files in the download directory
        initial_files = set(os.listdir(download_dir))
        self.logger.info(f"Files in download directory before download: {len(initial_files)}")
        
        # Try to find and click the download button
        download_target = self.find_download_button(driver)
        
        if download_target:
            # Check if download_target is a WebElement or URL string
            if hasattr(download_target, 'tag_name'):
                # We have a WebElement, click it
                self.logger.info("Clicking download button...")
                try:
                    download_target.click()
                    self.logger.info("Clicked download button")
                except Exception as e:
                    self.logger.error(f"Error clicking button: {e}")
                    try:
                        # Try JavaScript click as fallback
                        driver.execute_script("arguments[0].click();", download_target)
                        self.logger.info("Used JavaScript to click download button")
                    except Exception as js_e:
                        self.logger.error(f"JavaScript click also failed: {js_e}")
                        return None
            else:
                # Assume it's a URL string
                self.logger.info(f"Navigating directly to export URL: {download_target}")
                current_url = driver.current_url
                full_url = f"https://dashboard.tevi.tech{download_target}"
                driver.get(full_url)
                time.sleep(2)  # Wait for download to start
                driver.get(current_url)  # Go back to results page
        else:
            self.logger.warning("Could not find download button.")
            return None
            
        # Wait for download to complete
        start_time = time.time()
        while time.time() - start_time < timeout:
            if stop_event and stop_event.is_set():
                self.logger.warning("Download cancelled by stop event")
                return None
                
            time.sleep(1)
            current_files = set(os.listdir(download_dir))
            new_files = current_files - initial_files
            
            # Check for new CSV files
            csv_files = [f for f in new_files if f.endswith('.csv')]
            if csv_files:
                # Sort by creation time, newest first
                csv_files.sort(key=lambda x: os.path.getctime(os.path.join(download_dir, x)), reverse=True)
                csv_path = os.path.join(download_dir, csv_files[0])
                
                # Make sure the file is fully downloaded (not a partial download)
                if not os.path.exists(csv_path + '.crdownload') and not os.path.exists(csv_path + '.part'):
                    # Verify file size is stable (not still being written)
                    size1 = os.path.getsize(csv_path)
                    time.sleep(1)  # Wait a bit to check if size changes
                    size2 = os.path.getsize(csv_path)
                    
                    if size1 == size2:  # File size is stable
                        self.logger.info(f"Download completed successfully: {csv_path}")
                        return csv_path

    def setup_sql_environment(self, driver, database_name, schema_name, continue_on_error=False):
        """Set up the SQL environment by selecting database and schema."""
        self.logger.info(f"Setting up SQL environment: database={database_name}, schema={schema_name}")
        
        # Select database
        database_selected = self.select_database(driver, database_name)
        if not database_selected and not continue_on_error:
            manual_continue = input("Could not select database. Do you want to continue? (y/n): ")
            if manual_continue.lower() != 'y':
                self.logger.info("User chose to abort after database selection failure")
                return False
        
        # Select schema
        schema_selected = self.select_schema(driver, schema_name)
        if not schema_selected and not continue_on_error:
            manual_continue = input("Could not select schema. Do you want to continue? (y/n): ")
            if manual_continue.lower() != 'y':
                self.logger.info("User chose to abort after schema selection failure")
                return False
        
        self.logger.info("SQL environment setup complete")
        return database_selected and schema_selected

    def sql_workflow(self, driver=None, stop_event=None, url=None, query=None):
        """Complete SQL workflow: navigate, setup environment, run query."""
        try:
            # Navigate to SQL page
            self.get_to_site(url, driver, stop_event)
            
            # Set up SQL environment
            success = self.setup_sql_environment(
                driver=driver, 
                database_name="Airdrop campaign", 
                schema_name="public"
            )
            
            if not success:
                self.logger.warning("SQL environment setup failed or was aborted")
                return
            
            # If a query was provided, run it
            if query:
                query_success = self.run_sql_query(driver, query)
                if query_success:
                    self.logger.info("SQL query executed successfully")
                else:
                    self.logger.warning("SQL query execution failed")
            
        except Exception as e:
            self.logger.error(f"Error in SQL workflow: {e}")


class LocalSelenium:
    def __init__(self, cookie_path='cookies.pkl'):
        self.cookie_path=cookie_path
        self.driver=None
        pass

    def _get_selenium_driver(self):
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        # Create a persistent Chrome profile directory
        user_data_dir = os.path.join(os.getcwd(), "chrome_profile")
        if not os.path.exists(user_data_dir):
            os.makedirs(user_data_dir)

        # Add the user data directory to Chrome options
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--profile-directory=Default")

        # Additional options to improve session persistence
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-site-isolation-trials")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        prefs = {
            "credentials_enable_service": True,
            "profile.password_manager_enabled": True,
            "autofill.profile_enabled": True,
            "download.default_directory": os.path.join(os.getcwd(), "downloads"),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        options.add_experimental_option("prefs", prefs)

        try:
            driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
                """
            })
        except Exception as e:
            driver=None
            print(f'Error create selenium driver: {e}')
        self.driver=driver

    def init_selenium(self):
        self._get_selenium_driver()

    def check_selenium_driver(self):
        if self.driver is None:
            self.init_selenium()
        return self.driver

    def get_to_site(self, url="https://dashboard.tevi.tech/sqllab/", driver=None, stop_event=None):
        driver=self.check_selenium_driver()
        if not driver:
            print('Driver not available!')
            return
        
        self.load_cookies() 

        try:
            print("Navigating to Tevi dashboard...")
            driver.get(url=url)
        except Exception as e:
            print(f'Error getting to {url}: {e}')
    
    def site_login_handle(self):
        driver=self.check_selenium_driver()
        if not driver:
            print('Driver not available!')
            return False
        if "/login" in driver.current_url or "sign in" in driver.page_source.lower() or "username" in driver.page_source.lower():
            print("Detected login page")
            autofill_login=self.superset_login_autofill()
            if autofill_login:
                self.save_cookies()
            else:
                print('Please login manually')

        elif "cloudflareaccess.com" in driver.current_url:
            print("Detected Cloudflare Access page")

        else:
            print('Login not detected!')

    def superset_login_autofill(self):
        driver=self.check_selenium_driver()
        if not driver:
            print('Driver not available!')
            return False
        
        username, password = self.load_credentials()

        try:
            username_field = driver.find_element(By.XPATH, "//input[@name='username' or contains(@placeholder, 'Username')]")
            password_field = driver.find_element(By.XPATH, "//input[@name='password' or @type='password' or contains(@placeholder, 'Password')]")
            
            username_field.clear()
            username_field.send_keys(username)
            password_field.clear()
            password_field.send_keys(password)

            print(f'Autofill username and password successfully')
        
        except Exception as e:
            print(f'Error autofill username and password')
            return False
        

        login_button = None
        
        # Method 1: Standard button selectors
        button_selectors = [
            "//button[contains(text(), 'SIGN IN') or contains(text(), 'Sign In') or contains(text(), 'Login') or contains(text(), 'LOG IN')]",
            "//button[@type='submit']",
            "//input[@type='submit']",
            "//button[contains(@class, 'btn-primary')]",
            "//button[contains(@class, 'login')]",
            "//button"  # Last resort: just find any button
        ]
        
        for selector in button_selectors:
            try:
                buttons = driver.find_elements(By.XPATH, selector)
                if buttons:
                    print(f"Found {len(buttons)} buttons with selector: {selector}")
                    for button in buttons:
                        print(f"Button text: '{button.text}', class: '{button.get_attribute('class')}'")
                    login_button = buttons[0]
                    break
            except:
                continue
        
        # Method 2: If no button found, try to submit the form directly
        if not login_button:
            try:
                print("No button found, trying to submit the form directly...")
                form = driver.find_element(By.XPATH, "//form")
                driver.execute_script("arguments[0].submit();", form)
                print("Form submitted via JavaScript")
                time.sleep(5)
                return True
            except Exception as form_e:
                print(f"Error submitting form: {form_e}")
                
                # Method 3: Last resort - click the blue button if it exists
                try:
                    print("Trying to find a blue button or any button that might be the login button...")
                    # Find buttons with blue-ish styling
                    blue_buttons = driver.find_elements(By.XPATH, 
                        "//button[contains(@style, 'blue') or contains(@class, 'blue')] | " +
                        "//button[contains(@style, 'primary') or contains(@class, 'primary')] | " +
                        "//input[@type='submit']"
                    )
                    
                    if blue_buttons:
                        login_button = blue_buttons[0]
                        print(f"Found potential login button with text: {login_button.text}")
                    else:
                        # Find any submit-like button as a last resort
                        all_buttons = driver.find_elements(By.TAG_NAME, "button")
                        if all_buttons:
                            login_button = all_buttons[-1]  # Often the submit button is the last one
                            print(f"Using last button as login: {login_button.text}")
                        else:
                            print("No buttons found on page!")
                except Exception as e:
                    print(f"Error finding any buttons: {e}")
        
        # Click the login button if we found one
        if login_button:
            try:
                print(f"Clicking button with text: '{login_button.text}'")
                login_button.click()
                print("Login button clicked")
            except Exception as click_e:
                print(f"Error clicking button: {click_e}")
                # Try JavaScript click
                try:
                    driver.execute_script("arguments[0].click();", login_button)
                    print("Used JavaScript to click login button")
                except:
                    print("JavaScript click also failed")
        else:
            print("Could not find any login button!")
            driver.save_screenshot("no_login_button.png")
            return False
        
        print("Login form submitted. Waiting for page to load...")
        
        # Wait for redirection or login error
        time.sleep(5)




    def save_cookies(self):
        driver=self.check_selenium_driver()
        if not driver:
            print('Driver not available!')
            return False

        cookies=driver.get_cookies()
        cookie_path=self.cookie_path
        try:
            with open(cookie_path, 'wb') as file:
                pickle.dump(cookies, file)
            print(f"Cookies saved to {cookie_path}")
        except Exception as e:
            print(f'Error saving cookie to {cookie_path}: {e}')


    def load_cookies(self):
        driver=self.check_selenium_driver()
        if not driver:
            print('Driver not available!')
            return False
    
        cookie_path=self.cookie_path
        if not os.path.exists(cookie_path):
            print(f"Cookie file in {cookie_path} not found")
            return False
        
        add_cookie_error = 0
        try:
            with open(cookie_path, 'rb') as file:
                cookies=pickle.load(file)
                for cookie in cookies:
                    try:
                        driver.add_cookie(cookie)
                    except Exception as e:
                        add_cookie_error += 1
                        print(f'Error adding cookie: {e}')
                        print(f'Error cookie: {cookie}')
        except Exception as e:
            print(f'Error open and add cookies: {e}')
            return False

        if add_cookie_error == 0:
            print("Cookies loaded successfully")
        else:
            print(f"Cookies loaded successfully with {add_cookie_error} error(s)")

    def save_credentials(self):
        """Save credentials to an encrypted file."""
        print("First-time setup: Please enter your Tevi dashboard credentials")
        username = input("Username: ")
        password = getpass.getpass("Password: ")
        
        # Simple encryption (not secure, but better than plaintext)
        import base64
        encoded_username = base64.b64encode(username.encode()).decode()
        encoded_password = base64.b64encode(password.encode()).decode()
        
        credentials_file = "tevi_creds.dat"
        with open(credentials_file, "w") as f:
            f.write(f"{encoded_username}\n{encoded_password}")
        
        print("Credentials saved.")
        return username, password

    def load_credentials(self):
        """Load credentials from file."""
        credentials_file = "tevi_creds.dat"
        
        if not os.path.exists(credentials_file):
            return self.save_credentials()
        
        try:
            import base64
            with open(credentials_file, "r") as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    encoded_username = lines[0].strip()
                    encoded_password = lines[1].strip()
                    
                    username = base64.b64decode(encoded_username.encode()).decode()
                    password = base64.b64decode(encoded_password.encode()).decode()
                    
                    return username, password
        except Exception as e:
            print(f"Error loading credentials: {e}")
            return self.save_credentials()
        
        # If we get here, something went wrong with the file format
        return self.save_credentials()

    def quit_driver(self):
        driver=self.check_selenium_driver()
        driver.quit()
