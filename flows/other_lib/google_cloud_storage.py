from .auth_and_token import GoogleAuthManager
from typing import Literal
import os
import io

class GoogleCloudStorage:
    def __init__(self, client_secret_directory, use_service_account = True):
        self.use_service_account = use_service_account
        self.auth_manager = GoogleAuthManager(client_secret_directory, use_service_account)
        self.client_secret_directory = client_secret_directory
        self.gcs_client = None

    def get_gcs_client(self):
        self.auth_manager.check_cred()
        self.gcs_client = self.auth_manager.credentials.gcs_client

    def upload_file_to_gcs(self, source_file_path, bucket_folder_path):
        parts = bucket_folder_path.strip('/').split('/', 1)
        bucket_name = parts[0]
        self.get_gcs_client()
        gcs_client = self.gcs_client

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
    
    def download_file(
        self, 
        bucket_name, 
        source_blob_name: Literal['file path in GCS'], 
        destination_file_path: Literal['file path in local'] = None
    ):

        self.get_gcs_client()
        gcs_client = self.gcs_client
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # Determine filename from blob path
        filename = os.path.basename(source_blob_name)

        # If destination path not provided, save to CWD with original filename
        if not destination_file_path:
            path = os.path.join(os.getcwd(), filename)
        elif os.path.isdir(destination_file_path):
            # If user passed a folder path instead of file path
            path = os.path.join(destination_file_path, filename)
        else:
            raise ValueError(f"Invalid destination_file_path: {destination_file_path}")

        destination_file_path=path

        # Create the destination folder if needed
        os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)

        # Download the file
        blob.download_to_filename(destination_file_path)

        # Clean print path
        try:
            relative_path = os.path.relpath(destination_file_path)
        except ValueError:
            relative_path = destination_file_path

        print(f"✅ Downloaded: gs://{bucket_name}/{source_blob_name}")
        print(f"📁 Saved to: {relative_path}")
        
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
        self.get_gcs_client()
        gcs_client = self.gcs_client

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
        self.get_gcs_client()
        gcs_client = self.gcs_client
        bucket = gcs_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return blobs
        # return [blob.name for blob in blobs]