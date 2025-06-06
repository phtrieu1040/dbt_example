from .google_bqr import BigqueryQuery, BigqueryTable
from .google_mail import GoogleMail
from .google_file import GoogleFileManager, GoogleFileEditor, GoogleDriveFileManager
from .google_cloud_storage import GoogleCloudStorage
from .google_cloud_platform import GoogleCloudPlatform
from .google_vpc import GoogleVPC
from .utility_lib import MyFunction, Telegram

class MyLibrary:
    def __init__(self,client_secret_directory, use_service_account = True) -> None:
        # client_secret_directory = r'C:\trieu.pham\python\bigquery'
        # client_secret_directory = r'C:\Python\file_token'
        self._bigquery_qr = BigqueryQuery(client_secret_directory, use_service_account)
        self._bigquery_table = BigqueryTable(client_secret_directory, use_service_account)
        self._google_mail = GoogleMail(client_secret_directory, use_service_account)
        self._google_file = GoogleFileManager(client_secret_directory, use_service_account)
        self._google_file_editor = GoogleFileEditor(client_secret_directory, use_service_account)
        self._google_drive_file = GoogleDriveFileManager(client_secret_directory, use_service_account)
        self._google_cloud_storage = GoogleCloudStorage(client_secret_directory, use_service_account)
        self._google_cloud_platform = GoogleCloudPlatform(client_secret_directory, use_service_account)
        self._google_vpc = GoogleVPC(client_secret_directory, use_service_account)
        self._telegram = Telegram()
    
    @property
    def bigquery_qr(self):
        return self._bigquery_qr
    
    @property
    def bigquery_table(self):
        return self._bigquery_table
    
    @property
    def google_mail(self):
        return self._google_mail
    
    @property
    def google_file(self):
        return self._google_file
    
    @property
    def google_file_editor(self):
        return self._google_file_editor
    
    @property
    def google_drive_file(self):
        return self._google_drive_file
    
    @property
    def google_cloud_storage(self):
        return self._google_cloud_storage
    
    @property
    def google_cloud_platform(self):
        return self._google_cloud_platform
    
    @property
    def google_vpc(self):
        return self._google_vpc
    
    @property
    def telegram(self):
        return self._telegram

    @property
    def function(self):
        return MyFunction()