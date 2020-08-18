from google.cloud import bigquery, storage
import pandas as pd
import os
import json

def df_to_gbq(df, dataset, table_name):
    """Load a pandas DataFrame into a specified BigQuery table."""
    bq_client = bigquery.Client(project="modalitydashboards")
    table_id = f"modalitydashboards.{dataset}.Budget2"

    # Open and load json metadata file
    with open("metadata.json") as json_file:
        data = json.load(json_file)
        metadata = data["Budget"]

    # Create BigQuery schema from json metadata
    schema = [bigquery.SchemaField(entry['bq_name'], entry['bq_dtype']) for entry in metadata if entry['bq_name'] != None]

    job_config = bigquery.LoadJobConfig(schema=schema)

    # Load DataFrame in BigQuery
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for load job to complete
    job.result()

def data_file_info(string):
    """Split file uploaded to google cloud bucket into a filepath and filename."""
    path, filename = os.path.split(string)
    return path, filename

def delete_blob(bucket_name, blob_name):
    """Deletes a blob (file) from a bucket."""
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print(f"{blob_name} deleted.")
