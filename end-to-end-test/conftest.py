import socket
import time
import string
import random
import pytest
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from google.cloud import storage as google_storage
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class TempBucketFactory:
    def __init__(self):
        self.s3_bucket_names = []
        self.gs_bucket_names = []
        self.abs_container_names = []

    def make_name(self):
        return "keepsake-test-endtoend-" + "".join(
            random.choice(string.ascii_lowercase) for _ in range(20)
        )

    def s3(self):
        name = self.make_name()
        self.s3_bucket_names.append(name)
        return name

    def gs(self):
        name = self.make_name()
        self.gs_bucket_names.append(name)
        return name

    def abs(self):
        name = self.make_name()
        self.abs_container_names.append(name)
        return name

    def cleanup(self):
        if self.s3_bucket_names:
            s3 = boto3.resource("s3")
            for bucket_name in self.s3_bucket_names:
                bucket = s3.Bucket(bucket_name)
                bucket.objects.all().delete()
                bucket.delete()
        if self.gs_bucket_names:
            storage_client = google_storage.Client()
            for bucket_name in self.gs_bucket_names:
                bucket = storage_client.bucket(bucket_name)
                for blob in bucket.list_blobs():
                    blob.delete()
                bucket.delete()
        if self.abs_container_names:
            # This credential first checks environment variables for configuration as described above.
            # If environment configuration is incomplete, it will try managed identity.
            credential = DefaultAzureCredential()
            account_url = os.environ["STORAGE_BLOB_URL"]

            blob_service_client = BlobServiceClient(account_url, credential=credential)

            for container_name in self.abs_container_names:
                container_client = blob_service_client.get_container_client(container_name)
                if container_client.exists():
                    for blob in container.list_blobs():
                        container_client.delete_blob(blob, delete_snapshots="include")
                    container_client.delete_container()


@pytest.fixture(scope="function")
def temp_bucket_factory() -> TempBucketFactory:
    # We don't create bucket here so we can test Keepsake's ability to create
    # buckets.
    factory = TempBucketFactory()
    yield factory
    factory.cleanup()


def wait_for_port(port, host="localhost", timeout=5.0):
    """Wait until a port starts accepting TCP connections.
    Args:
        port (int): Port number.
        host (str): Host address on which the port should exist.
        timeout (float): In seconds. How long to wait before raising errors.
    Raises:
        TimeoutError: The port isn't accepting connection after time specified in `timeout`.
    """
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(1.0)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError(
                    "Waited too long for the port {} on host {} to start accepting connections.".format(
                        port, host
                    )
                ) from ex
