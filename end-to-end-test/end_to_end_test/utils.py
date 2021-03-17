import os
from pathlib import Path
import boto3
import botocore
from google.cloud import storage as google_storage
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

ROOT_DIRECTORY = Path(__file__).parent.parent.parent


def get_env():
    """
    Returns environment for running Keepsake commands in
    """
    env = os.environ
    env["PATH"] = os.environ["PATH"] + ":/usr/local/bin"
    return env


def path_exists(repository, path):
    """
    Check that a path exists in a specific repository.
    repository has the format backend://root(/parent_folder),
    where backend can be file, s3, gs, or abs.
    """
    backend, root = repository.split("://")
    assert backend in ("file", "s3", "gs", "abs")

    if backend == "file":
        return (Path(root) / path).exists()

    # append any parent_folder that's part of the repository string
    # to the path, so that root is just the bucket
    root = Path(root)
    if len(root.parts) > 1:
        path = Path(*root.parts[1:]) / path
    root = root.parts[0]

    if backend == "s3":
        s3 = boto3.resource("s3")
        try:
            s3.Object(root, str(path)).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise
    if backend == "gs":
        storage_client = google_storage.Client()
        bucket = storage_client.bucket(root)
        blob = bucket.blob(str(path))
        return blob.exists()
    if backend == "abs":
        # https://azuresdkdocs.blob.core.windows.net/$web/python/azure-identity/1.3.0/index.html#authenticating-with-defaultazurecredential
        credential = DefaultAzureCredential()
        account_url = os.environ["STORAGE_BLOB_URL"]
        try:
            blob_service_client = BlobServiceClient(account_url, credential=credential) # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
            container_client = blob_service_client.get_container_client(root) # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
            blob_client = container_client.get_blob_client(str(path)) # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#get-blob-client-blob--snapshot-none-
            return blob_client.exists() # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python#exists---kwargs-
        except Exception as e:
            raise e
