import os
from google.cloud import storage, bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "fake_secret_key.json"
storage_client = storage.Client()
bigquery_client = bigquery.Client()

schema = [
        bigquery.SchemaField("quadkey", "INTEGER"),
        bigquery.SchemaField("avg_d_kbps", "INTEGER"),
        bigquery.SchemaField("avg_u_kbps", "INTEGER"),
        bigquery.SchemaField("avg_d_mbps", "FLOAT64"),
        bigquery.SchemaField("avg_u_mbps", "FLOAT64"),
        bigquery.SchemaField("avg_lat_ms", "INTEGER"),
        bigquery.SchemaField("tests", "INTEGER"),
        bigquery.SchemaField("devices", "INTEGER"),
        bigquery.SchemaField("long1", "FLOAT64"),
        bigquery.SchemaField("lat1", "FLOAT64"),
        bigquery.SchemaField("long2", "FLOAT64"),
        bigquery.SchemaField("lat2", "FLOAT64"),
        bigquery.SchemaField("long3", "FLOAT64"),
        bigquery.SchemaField("lat3", "FLOAT64"),
        bigquery.SchemaField("long4", "FLOAT64"),
        bigquery.SchemaField("lat4", "FLOAT64"),
        bigquery.SchemaField("long5", "FLOAT64"),
        bigquery.SchemaField("lat5", "FLOAT64"),
        bigquery.SchemaField("Quarter", "STRING"),
        bigquery.SchemaField("Year", "INTEGER"),
        bigquery.SchemaField("SpeedID", "INTEGER"),
        bigquery.SchemaField("CoordinateID", "INTEGER"),
        bigquery.SchemaField("DateID", "INTEGER")
    ]

# Use this function once when you haven't create your bucket
def create_google_bucket():
    storage_client.create_bucket('ookla_bucket', location = 'asia')

# Use this function once when you haven't create the dataset
def create_dataset(name):
    # create dataset
    dataset_id = f"{bigquery_client.project}.{name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'asia-southeast1'
    dataset = bigquery_client.create_dataset(dataset, timeout=30)

# Use this function once when you haven't create the table (only for Staging purpose)
def create_table():
    # create table
    table_id = f"{bigquery_client.project}.Staging.stg"
    
    table = bigquery.Table(table_id, schema = schema)
    table = bigquery_client.create_table(table)

# Only for Staging
def bucketToBigQuery():
    blobs = storage_client.list_blobs('ookla_bucket')

    job_config = bigquery.LoadJobConfig(
        schema = schema,
        skip_leading_rows = 1,
        source_format = bigquery.SourceFormat.CSV
    )

    table_id = f"{bigquery_client.project}.Staging.stg"

    for blob in blobs:
        uri = f"gs://ookla_bucket/{blob.name}"

        print(uri)

        load_job = bigquery_client.load_table_from_uri(
            uri, table_id, job_config = job_config
        )

        load_job.result()