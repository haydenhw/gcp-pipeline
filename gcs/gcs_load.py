from google.cloud import storage
import os

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        print(blob.name)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print("Uploading file...")
    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def upload_dir(src_dir_name, target_bucket_name):
    for file_name in os.listdir(src_dir_name):
        src_path = f"{src_dir_name}/{file_name}"
        print(src_path)
        upload_blob(target_bucket_name, src_path, file_name)


if __name__ == "__main__":
    bucket_name = "yob-7913"
    src_dir = "../staging"
    upload_dir(src_dir, bucket_name)
