from google.cloud import storage


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))


def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def archive_blobs(bucket_name, archive_bucket_name="archive-7913"):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    print(f"Archiving blobs in bucket {bucket_name}...")
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        copy_blob(bucket_name, blob.name, archive_bucket_name, blob.name)
        delete_blob(bucket_name, blob.name)


if __name__ == "__main__":
    blob_name = "yob2017.avro"
    src_bucket = "yob-7913"
    archive_bucket = "archive-7913"
    copy_blob(src_bucket, blob_name, archive_bucket, blob_name)
    delete_blob(src_bucket, blob_name)