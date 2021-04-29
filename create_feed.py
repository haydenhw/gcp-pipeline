# create a new bucket
# create a new table

from gcs.gcs_mb import create_bucket

if __name__ == "__main__":
    bucket_name = "yob-7913"

    print(f"Creating bucket '{bucket_name}' ...")
    create_bucket(bucket_name)
