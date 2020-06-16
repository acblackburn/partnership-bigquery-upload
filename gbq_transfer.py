from google.cloud import bigquery, storage
import pandas as pd
import os

def gcs_to_df(bucket_name):
    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name)

def df_to_gbq(df, table_name):
    bq_client = bigquery.Client()
    table_id = "example-data-pipeline.gbq_test." + table_name
    schema = []

    # Identify dataframe columns of a object (string) datatype
    df_dtypes = df.dtypes
    for column, dtype in df_dtypes.items():
        if dtype == "object":
            schema.append(bigquery.SchemaField(column, "STRING"))
    job_config = bigquery.LoadJobConfig(schema=schema)

    # Load DataFrame in BigQuery
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for load job to complete
    job.result()

def event_file_info(string):
    path, filename = os.path.split(string)
    return (path, filename)