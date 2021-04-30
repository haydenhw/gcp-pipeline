import os
import sys
from transform.csv_to_avro import write_avro, read_avro
from gcs.gcs_load import upload_dir
from bq.bq_load_avro import load_avro
from gcs.gcs_archive import archive_blobs

def find_most_recent_year():
    data_dir = os.path.join("data", "names")
    files = os.listdir(data_dir)
    last_file = files[-1:][0]
    year = last_file.split(".")[0][-4:]
    return year

def run_names_job():
    staging_bucket = "yob-7913"
    staging_dir = "staging"
    year = find_most_recent_year()

    file_in = os.path.join("data", "names", f"yob{year}.txt")
    file_out = os.path.join("staging", f"yob{year}.avro")
    write_avro(
        file_in,
        file_out,
        schema_path=os.path.join("data", "yob.avsc")
    )
    read_avro(file_out)
    os.remove(file_in)

    # upload files into GCS
    upload_dir(staging_dir, staging_bucket)
    os.remove(file_out)

    # load files into BQ
    load_avro(
        data_uri=f"gs://{staging_bucket}/*.avro",
        dataset_name="mig_practice",
        table_name="yob"
    )

    # archive files
    archive_blobs(staging_bucket)

    sys.exit(0)

if __name__ == "__main__":
  run_names_job()