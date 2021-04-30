from google.cloud import bigquery

def load_avro(data_uri, dataset_name, table_name):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = "seventh-torch-311320.{}.{}".format(dataset_name, table_name)

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)

    load_job = client.load_table_from_uri(
        data_uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

if __name__ == "__main__":
    data_uri = "gs://yob-7913/*.avro"
    dataset_name="mig_practice"
    table_name="yob"
    load_avro(data_uri, dataset_name, table_name)