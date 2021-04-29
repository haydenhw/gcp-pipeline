from google.cloud import bigquery

def create_dataset(dataset_name):
  # Construct a BigQuery client object.
  client = bigquery.Client()

  # TODO(developer): Set dataset_id to the ID of the dataset to create.
  dataset_id = "{}.{}".format(client.project, dataset_name)

  # Construct a full Dataset object to send to the API.
  dataset = bigquery.Dataset(dataset_id)

  dataset.location = "US"

  # Send the dataset to the API for creation, with an explicit timeout.
  # Raises google.api_core.exceptions.Conflict if the Dataset already
  # exists within the project.
  dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
  print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


if __name__ == "__main__":
  dataset_name = "mig_practice"
  create_dataset(dataset_name)