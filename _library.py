from __future__ import print_function
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import credentials
from google.cloud import bigquery
from google.cloud import storage
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


SCOPES=["https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
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

class GoogleCloudStorage:
    def __init__(self, client_secret_directory, use_service_account=True):
        self.use_service_account = use_service_account
        self.client_secret_file_path = os.path.join(client_secret_directory, client_secret_file)
        self._storage_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        if self.use_service_account:
            self._storage_client = storage.Client.from_service_account_json(self.client_secret_file_path)
        else:
            self._storage_client = storage.Client()
    
    @property
    def client(self):
        if not self._storage_client:
            self._initialize_client()
        return self._storage_client
    

    def upload_file_to_gcs(self, source_file_path, bucket_folder_path):
        parts = bucket_folder_path.strip('/').split('/', 1)
        bucket_name = parts[0]
        
        filename = os.path.basename(source_file_path)
        
        if len(parts) > 1:
            folder_path = parts[1]
            if not folder_path.endswith('/'):
                folder_path += '/'
            destination_blob_name = f"{folder_path}{filename}"
        else:
            destination_blob_name = filename
        
        bucket = self.client.bucket(bucket_name)
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
        return f"gs://{bucket_name}/{destination_blob_name}"
    

    def download_file(self, bucket_name, source_blob_name:Literal['file path in GCS'], destination_file_path:Literal['file path in local']=None):
        bucket = self.client.bucket(bucket_name)
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
        bucket = self.client.bucket(bucket_name)
        
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
        bucket = self.client.bucket(bucket_name)
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

        with open(file_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = self.client.get_table(table_id)  # Make an API request.
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
