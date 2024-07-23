from google.cloud import storage

from config import GCSConfig


class GCSClient:
    storage_client = storage.Client.from_service_account_json(GCSConfig.CREDENTIALS)

    @staticmethod
    def upload_file(source_file_path: str, destination_blob_name: str):
        """
        Uploads a file to GCS.

        Parameters:
        source_file_path (str): Path to the file to upload.
        destination_blob_name (str): Destination path in GCS.

        Returns:
        None
        """
        bucket = GCSClient.storage_client.bucket(GCSConfig.BUCKET_NAME)

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)

    @staticmethod
    def download_file(source_blob_name: str, destination_file_path: str):
        """
        Downloads a file from GCS.

        Parameters:
        source_blob_name (str): Path to the file in GCS.
        destination_file_path (str): Destination path on local filesystem.

        Returns:
        None
        """
        bucket = GCSClient.storage_client.bucket(GCSConfig.BUCKET_NAME)

        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_path)
