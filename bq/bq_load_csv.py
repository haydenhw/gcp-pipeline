from google.cloud import bigquery

client = bigquery.Client()

table_id = "seventh-torch-311320.deniro.deniro"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("sex", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)


if __name__ == "__main__":
    uri = "gs://new-bucket-7913/yob2019.txt"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
