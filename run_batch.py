import os
import sys
from transform.csv_to_avro import write_avro, read_avro
from gcs.gcs_load import upload_dir
from bq.bq_load_avro import load_avro
from gcs.gcs_archive import archive_blobs
from util.db import get_connection
from services.names import  list_names_by_year
from services.extraction_logger import get_last_year_processed, log_current_year
from services import crypto

def find_most_recent_year():
    data_dir = os.path.join("data", "names")
    files = os.listdir(data_dir)
    last_file = files[-1:][0]
    year = last_file.split(".")[0][-4:]
    return year




def run_names_job(pg_con):
    staging_bucket = "yob-7913"
    staging_dir = "staging"

    # read names from DB
    # log having done so
    year = int(get_last_year_processed()) + 1
    names = list_names_by_year(pg_con, year)
    log_current_year(year)

    # convert result set to an avro file
    file_out = os.path.join("staging", f"yob{year}.avro")
    write_avro(
        names,
        file_out,
        schema_path=os.path.join("data", "yob.avsc")
    )
    read_avro(file_out)

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
    run_names_job(pg_con)
