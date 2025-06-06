from .utility_lib import MyFunction
from .auth_and_token import GoogleAuthManager
import re
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from xlsxwriter.utility import xl_cell_to_rowcol, xl_rowcol_to_cell
from typing import Literal
import os
import pandas as pd

class GoogleFileManager:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = self.auth_manager.client_secret_directory
        self.sheet = None
        self.credentials = None

    def _get_sheet(self, url):
        if url:
            sheet = self.auth_manager._get_sheet(url=url)
            self.sheet = sheet

    def _get_credentials(self):
        self.auth_manager.check_cred()
        self.credentials = self.auth_manager.credentials

    def get_permissions_email_from_google_sheet(self, url):
        self._get_sheet(url=url)
        permissions = self.sheet.list_permissions()
        df_permissions = pd.DataFrame(permissions)
        emails = df_permissions.emailAddress.tolist()
        return emails
    
    def get_full_permissions_from_google_sheet_df(self, url):
        self._get_sheet(url=url)
        permissions = self.sheet.list_permissions()
        df_permissions = pd.DataFrame(permissions)
        return df_permissions
    
    def get_full_permissions_from_google_sheet_raw(self, url):
        self._get_sheet(url=url)
        permissions = self.sheet.list_permissions()
        return permissions
    
    def transfer_ownership(self, email, file_id, notify=True):
        self._get_credentials()
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
        self._get_credentials()
        self.credentials.gspread_client.del_spreadsheet(file_id=file_id)

    def get_ggs_ids(self):
        self._get_credentials()
        a = list(self.credentials.gspread_client.list_spreadsheet_files())
        return a
    
    def create_new_google_sheet(self, sheet_name, email_to_share, perm_type = 'user', role = 'writer'):
        self._get_credentials()
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
        self._get_sheet(url=url)
        sheet = self.sheet
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

    def _remove_google_sheet_permission(self, sheet, email):
        sheet = sheet
        try:
            sheet.remove_permissions(email)
        except Exception as e:
            print(f"Error deleting permission '{email}': {e}")

    def remove_ggs_permissions(self, url, email):
        self._get_sheet(url)
        sheet = self.sheet
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
        self._get_sheet(url)
        sheet = self.sheet
        if type(email) is list:
            for email in email:
                self._grant_google_sheet_permission(sheet=sheet, email=email, role=role, notify=notify)
        else:
            self._grant_google_sheet_permission(sheet=sheet, email=email, role=role, notify=notify)

    def _get_file_worksheet_list(self, url):
        self._get_sheet(url)
        sheet = self.sheet
        worksheets = sheet.worksheets()
        worksheet_names = [worksheet.title for worksheet in worksheets]
        return worksheet_names
    
    def is_worksheet_completely_empty(self, url, worksheet_name):
        self._get_sheet(url)
        sh = self.sheet
        wks = sh.worksheet(worksheet_name)
        # Fetch all values
        all_values = wks.get_all_values()
        # Check if all values are empty
        if not all_values or all([not cell for row in all_values for cell in row]):
            return True
        else:
            return False

    def duplicate_google_sheet(self, source_id, target_file_name, email_to_share=None, copy_permissions=True, source_sheet_to_duplicate=None):
        self.auth_manager.check_cred()
        sheet = self.credentials.gspread_client.copy(source_id, title=target_file_name, copy_permissions=copy_permissions)
        if sheet is None:
            raise Exception("Failed to duplicate google sheet")
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

class GoogleFileEditor:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.ggf_manager = GoogleFileManager(client_secret_directory, use_service_account)
        self.client_secret_directory = self.auth_manager.client_secret_directory
        self.sheet = None
        self.service_sheet = None

    def _get_sheet(self, url):
        if url:
            sheet = self.auth_manager._get_sheet(url=url)
            self.sheet = sheet
        self.auth_manager.check_cred()
        self.service_sheet = self.credentials.service.spreadsheets()
    
    def read_google_sheet_by_url(self, url, worksheet_name='', header_row=1, range_name=''):
        self._get_sheet(url)
        sh = self.sheet
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
    
    def write_google_sheet_by_url(self,
                                  df_to_write,
                                  url,
                                  worksheet_name,
                                  start_cell='A1',
                                  delete_before_write=True,
                                  ):
        self._get_sheet(url)
        sh = self.sheet
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
        wks.update(start_cell,
                   [df_to_write.columns.values.tolist()] + df_to_write.values.tolist(),
                   value_input_option='USER_ENTERED')
    
    def _clear_sheet_by_client(self, sheetID, sheet_name):
        self._get_sheet(url=None)
        sheet = self.service_sheet
        sheet.values().clear(spreadsheetId=sheetID, range=sheet_name).execute()
    
    def _add_new_worksheet(self, url, new_sheet_name):
        self._get_sheet(url=url)
        sh = self.sheet
        sh.add_worksheet(title=new_sheet_name, rows=100, cols=10)
    
    def read_google_sheet(self, sheetID, sheet_name, header_row = 1):
        self._get_sheet(url=None)
        sheet = self.service_sheet
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
        self._get_sheet(url=None)
        sheet = self.service_sheet
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
    
class GoogleDriveFileManager:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = self.auth_manager.client_secret_directory
        self.service_drive = None

    def _get_service_drive(self):
        self.auth_manager.check_cred()
        self.service_drive = self.auth_manager.credentials.drive_service

    def _get_drive_file(self, file_id):
        self._get_service_drive()
        file = self.service_drive.CreateFile({'id': file_id})
        return file

    def read_google_drive_xlsx(self, file_id, sheet_name):
        file = self._get_drive_file(file_id=file_id)
        file.GetContentFile('temp.xlsx')
        df = pd.read_excel('temp.xlsx', sheet_name=sheet_name, dtype = 'str')
        os.remove('temp.xlsx')
        return df

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
        self._get_service_drive()
        batch = self.service_drive.new_batch_http_request(callback=self._batch_transfer_ownership_callback)
        for file_id in file_id_list:
            new_permissions = {
                'type': type,
                'role': role,
                'emailAddress': email_to_transfer,
            }
            request_id = file_id  # Using file_id as the request_id for easy tracking
            batch.add(
                self.service_drive.permissions().create(
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
    
    def upload_file_to_drive(self, local_file_path, remote_file_name=None, file_id=None, file_url=None, mime_type=None):
        self._get_service_drive()

        if file_url and not file_id:
            try:
                file_id = MyFunction.extract_google_drive_id(file_url)
            except Exception as e:
                print('Could not extract file Id from URL: ', {file_url}, ". Error: ", e)
        
        if remote_file_name is None:
            remote_file_name = os.path.basename(local_file_path)

        if file_id is None and remote_file_name is not None:
            query = f'name = "{remote_file_name}" and trashed = false'
            results = self.service_drive.files().list(
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
                updated_file = self.service_drive.files().update(
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
                created_file = self.service_drive.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f'File uploaded successfully. File ID: "{created_file['id']}"')
                return created_file['id']
        except Exception as e:
            print(f'Error uploading file to Google Drive: "{e}"')
            return None

    def _modify_permision_by_drive_service(self, email, sheet_id, role:Literal['reader', 'writer', 'owner'], type: Literal['anyone', 'user'], transfer_ownership=False):
        self._get_service_drive()
        new_permissions = {
            'role': role,
            'type': type,
            'emailAddress': email,
        }
        try:
            self.service_drive.permissions().create(fileId=sheet_id,
                                                               body=new_permissions,
                                                               transferOwnership=transfer_ownership).execute()
        except Exception as e:
            print(e)

    def _get_files_and_email_of_owner_all_page(self, all_page=True):
        self._get_service_drive()
        mime_types = [
            "application/vnd.google-apps.spreadsheet",
            "application/vnd.google-apps.document",
            "application/vnd.google-apps.presentation",
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
        service = self.service_drive
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
    
    def read_google_drive_file(self, file_id, output_file: Literal['temp.csv', 'temp.xlsx'], worksheet_name=None, header_row=0):
        file_id = MyFunction.extract_google_drive_id(file_id)
        self._get_service_drive()
        file = self.service_drive.files().get_media(fileId=file_id)
        df = pd.DataFrame()
        with open(output_file, 'wb') as f:
            downloader = MediaIoBaseDownload(f, file)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f'Download {int(status.progress() * 100)}%.')
        
        try:
            if output_file == 'temp.csv':
                df = pd.read_csv(output_file, dtype='str')
            else:
                if output_file == 'temp.xlsx' and worksheet_name:
                    df = pd.read_excel(output_file, dtype='str', sheet_name=worksheet_name, header=header_row)
                else:
                    print('Incorrect output_file, stop process')
        except Exception as e:
            print(e)
        os.remove(output_file)
        return df