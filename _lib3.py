from __future__ import print_function
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
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
from datetime import datetime, date, timedelta
import pytz
import imaplib


SCOPES=["https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
client_secret=r"C:\trieu.pham\python\bigquery\client_secret.json"
os.chdir(r"C:\trieu.pham\python\bigquery")

client_token='token.pickle'
pydrive_token='pydrive_token.pickle'


class Tokenization:
    @staticmethod
    def load_cred(name):
        try:
            creds = pickle.load(open(name, 'rb'))
        except:
            return None
        return creds

    @staticmethod
    def create_cred(type):
        if type == 'client':
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        elif type == 'pydrive':
            gauth = GoogleAuth()
            gauth.DEFAULT_SETTINGS['client_config_file'] = client_secret
            gauth.LocalWebserverAuth()
            with open(pydrive_token, 'wb') as token:
                pickle.dump(gauth, token)

class Authorization:
    def __init__(self):
        checker_client = True
        checker_pydrive = True
        while checker_client:
            creds = Tokenization.load_cred(client_token)
            # gauth = Tokenization.load_cred(pydrive_token)
            if creds is not None and not creds.expired:
                checker_client = False
                continue
            elif (creds is not None and creds.expired) or creds is None:
                try:
                    os.remove(r"C:\trieu.pham\python\bigquery\{}".format(client_token))
                except Exception:
                    print('No Token, Now Create New Cred!')
                Tokenization.create_cred(type='client')
            else: break
            
        while checker_pydrive:
            gauth = Tokenization.load_cred(pydrive_token)
            if gauth is not None and not gauth.access_token_expired:
                checker_pydrive = False
                continue
            elif (gauth is not None and gauth.access_token_expired) or gauth is None:
                try:
                    os.remove(r"C:\trieu.pham\python\bigquery\{}".format(pydrive_token))
                except Exception:
                    print('No Drive Token, Now Create New Cred!')
                Tokenization.create_cred(type='pydrive')
            else: break
        
        self._client = bigquery.Client(project='tiki-analytics-dwh', credentials=creds)
        self._service = build('sheets', 'v4', credentials=creds)
        self._gauth = gauth
        self._drive = GoogleDrive(gauth)
        self._gspread_client = gspread.authorize(creds)
    
    @property
    def client(self):
        return self._client
    
    @property
    def service(self):
        return self._service
    
    @property
    def gauth(self):
        return self._gauth

    @property
    def drive(self):
        return self._drive
    
    @property
    def gspread_client(self):
        return self._gspread_client


class Googlesheet:
    def __init__(self):
        self._Credential = Authorization()

    @property
    def Credential(self):
        return self._Credential
    
    @Credential.setter
    def Credential(self, new_credential):
        self._Credential = new_credential

    def check_cred(self):
        creds = Tokenization.load_cred(client_token)
        gauth = Tokenization.load_cred(pydrive_token)
        if self.Credential.client._credentials.expired or self.Credential.gauth.access_token_expired or creds is None or gauth is None:
            self.Credential = Authorization()

    def _get_sheet(self, url):
        self.check_cred()
        sheet = self.Credential.gspread_client.open_by_url(url=url)
        return sheet
    
    def get_permissions_email_from_google_sheet(self, url):
        sheet = self._get_sheet(url=url)
        permissions = sheet.list_permissions()
        df_permissions = pd.DataFrame(permissions)
        emails = df_permissions.emailAddress.tolist()
        return emails
    
    def get_full_permissions_from_google_sheet(self, url):
        sheet = self._get_sheet(url=url)
        permissions = sheet.list_permissions()
        df_permissions = pd.DataFrame(permissions)
        return df_permissions
    
    def transfer_ownership(self, email, file_id):
        client = self.Credential.gspread_client
        try:
            client.insert_permission(file_id=file_id,
                                     value=email,
                                     perm_type='user',
                                     role='owner')
        except Exception as e:
            print(e + " for file {}".format(file_id))
    
    def delete_google_sheet_file(self, file_id):
        self.check_cred()
        self.Credential.gspread_client.del_spreadsheet(file_id=file_id)

    def get_ggs_ids(self):
        a = list(self.Credential.gspread_client.list_spreadsheet_files())
        return a
    
    def create_new_google_sheet(self, sheet_name, email_to_share, perm_type = 'user', role = 'writer'):
        sheet = self.Credential.gspread_client.create(sheet_name)
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
        sheet = self.Credential.gspread_client.copy(source_id, title=target_file_name, copy_permissions=copy_permissions)
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


    def read_google_sheet_by_url(self, url, worksheet_name='', header_row=1):
        sh = self._get_sheet(url)
        if worksheet_name:
            wks = sh.worksheet(worksheet_name)
        else:
            wks = sh.worksheet(0)
        values = wks.get_all_values()
        data = pd.DataFrame(values[header_row:], columns=values[header_row-1])
        return data
    
    def write_google_sheet_by_url(self, url, worksheet_name, df_to_write, start_cell='A1', delete_before_write=True, copy_head=True):
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
        file = self.Credential.drive.CreateFile({'id': file_id})
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
        service_sheet = self.Credential.service.spreadsheets()
        return service_sheet
    
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

class Bigquery:
    def __init__(self):
        self._Credential = Authorization()

    @property
    def Credential(self):
        return self._Credential
    
    @Credential.setter
    def Credential(self, new_credential):
        self._Credential = new_credential

    def check_cred(self):
        creds = Tokenization.load_cred(client_token)
        gauth = Tokenization.load_cred(pydrive_token)
        if self.Credential.client._credentials.expired or self.Credential.gauth.access_token_expired or creds is None or gauth is None:
            self.Credential = Authorization()
    
    def _get_bqr_client(self):
        self.check_cred()
        client = self.Credential.client
        return client

    def drop_bigquery_table(self,table_id):
        self._get_bqr_client().delete_table(table_id)

    def bigquery_operation(self,query):
        self._get_bqr_client().query(query)

    def run_biqquery_to_df(self,query):
        query_string = query
        query_job = self._get_bqr_client().query(query_string)
        df = query_job.to_dataframe()
        return df

    def update_table_from_dataframe(self,df,table_id,write_disposition,time_partitioning=None):
        if time_partitioning==None:
            table = self._update_table_from_dataframe_no_partition(df,table_id,write_disposition)
        else:
            if write_disposition == 'WRITE_TRUNCATE_PARTITION':
                table = self._update_table_from_dataframe_partititon(df,table_id,time_partitioning)
            else:
                table = self._update_table_from_dataframe_other(df,table_id,write_disposition,time_partitioning)

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

    def _update_table_from_dataframe_other(self,df,table_id,write_disposition,time_partitioning):
        field_partition = time_partitioning['field']
        type_partition = time_partitioning['type']
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

    def _update_table_from_dataframe_partititon(self,df,table_id,time_partitioning):
        field_partition = time_partitioning['field']
        type_partition = time_partitioning['type']
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

    def load_table_from_uri(self, uri, sheet_name, table_name):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.ExternalSourceFormat.GOOGLE_SHEETS,
            autodetect = True,
            skip_leading_rows=1,
            create_disposition = "CREATE_IF_NEEDED",
            write_disposition = 'WRITE_TRUNCATE'
        )
        self._get_bqr_client().load_table_from_uri(
            source_uris=uri,
            destination=table_name,
            job_config=job_config
        )

class MyLibrary:
    # def __init__(self, type: Literal['Bigquery', 'Google', 'UserDefined']) -> None:
    def __init__(self) -> None:
        self._bigquery = Bigquery()
        self._google = Googlesheet()
    
    @property
    def bigquery(self):
        return self._bigquery
    
    @property
    def google(self):
        return self._google
    
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
        
    
    
    """reserve for horizontal waterfall"""
# import plotly.graph_objects as go

# ccc_data = ccc_all[ccc_all['year_key'] == '2023'][['sub_cate_report', 'doc', 'tp_90', 'ap', 'ccc']].copy()
# ccc_data['ap'] = ccc_data['ap'] * -1
# text_list = ccc_data.iloc[0, 1:].astype(str).to_list()
# text_list[:] = [round(float(x), 2) for x in text_list]
# total_color = 'orange' if float(ccc_data.iloc[0, 4]) >= 0 else 'green'

# waterfall_ccc = go.Figure(go.Waterfall(
#     name='CCC',
#     orientation="h",  # Set orientation to "h" for horizontal
#     measure=["relative", "relative", "relative", "total"],
#     x=ccc_data.iloc[0, 1:].to_list(),
#     y=ccc_data.iloc[:, 1:].columns.to_list(),
#     textposition="auto",
#     text=text_list,
#     textfont=dict(size=16),
#     connector={"line": {"color": "rgb(63, 63, 63)"}},
#     increasing={"marker": {"color": "red"}},
#     decreasing={"marker": {"color": "green"}},
#     totals={"marker": {"color": total_color}}
# ))

# waterfall_ccc.add_shape(
#     type="line", line=dict(color="black", dash="dash"), opacity=0.5,
#     x0=0.001, x1=0.001, xref="x", y0=-0.4, y1=3.4, yref="y"  # Adjusted coordinates
# )
# waterfall_ccc.add_shape(
#     type="line", line=dict(color="black", dash="dash"), opacity=0.5,
#     x0=0.001, x1=0.001, xref="x", y0=-0.4, y1=3.4, yref="y"  # Adjusted coordinates
# )
# waterfall_ccc.add_shape(
#     type="line", line=dict(color="blue"), opacity=1,
#     x0=-0.4, x1=3.4, xref="x", y0=0.001, y1=0.001, yref="y"  # Adjusted coordinates
# )

# waterfall_ccc.update_layout(
#     title={
#         'text': "Cash Conversion Cycle Of ",
#         'font': {'size': 24, 'family': 'Arial'}
#     },
#     xaxis={
#         'title': {
#             'text': "Days",
#             'font': {'size': 16, 'family': 'Arial'}
#         },
#         'tickfont': {'size': 14, 'family': 'Arial'}
#     },
#     yaxis={
#         'title': {
#             'text': "Parameters",
#             'font': {'size': 16, 'family': 'Arial'}
#         },
#         'tickfont': {'size': 20, 'family': 'Arial'}
#     },
#     showlegend=True
# )
# waterfall_ccc.show()


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

    def extract_sheet_id(sheet_url):
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
        with open(file_path, mode=mode, newline='') as file:
            writer = csv.writer(file)
            writer.writerow(cls._to_list(data_to_write))
    
    @classmethod
    def _write_csv_log(cls, file_path, key_column, *args):
        _data = args + (cls._get_current_time('string'),) #add write time
        data = list(_data)
        print(data, args)
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
                key = data[0]
                non_key = data[1:-1]
                non_key_columns = cls.non_outer_join_a_vs_b(key, non_key)
                for column in non_key_columns:
                    column_name = input(f'Enter column name for column ({column}): ')
                    column_names.append(column_name)
            column_names_to_write = cls._to_list('row_number') + cls._to_list(key_column) + column_names + cls._to_list('write_time')
            mode = 'w'
            row_number = 1
            cls._write_csv(file_path=file_path, mode=mode, data_to_write=column_names_to_write)
        data_to_write = cls._to_list(row_number) + cls._to_list(data)
        cls._write_csv(file_path=file_path, mode='a', data_to_write=data_to_write)
        print(data_to_write)

    @classmethod
    def sheet_id_to_url(cls,sheets_id):
        base_url = "https://docs.google.com/spreadsheets/d/"
        return f"{base_url}{sheets_id}/edit"
    
    @classmethod
    def check_dup_df_column_name(cls,df):
        column_list = df.columns.tolist()
        dup_columns = pd.DataFrame({'column':column_list}).groupby('column').agg(count=('column','count')).reset_index().query('count>1').column.tolist()
        if len(dup_columns) == 0:
            print('ok')
            return
        for column in dup_columns:
            print(column)
    
    @classmethod
    def procedure_and_logging(cls, log_path, input_list, main_func, key_column='key_column'):
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


        if os.path.exists(log_path):
            all_logs = cls._read_csv(file_path=log_path)
            if all_logs is None:
                print('error reading log')
                return
            df_all_logs = pd.DataFrame(all_logs)
            completed_procedure = df_all_logs[key_column].to_list()
            remaining_procedure = MyFunction.non_outer_join_a_vs_b(a=completed_procedure, b=input_list)
        else:
            remaining_procedure = input_list
        for procedure in remaining_procedure:
            data = main_func(procedure)
            cls._write_csv_log(log_path, key_column, *data)

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