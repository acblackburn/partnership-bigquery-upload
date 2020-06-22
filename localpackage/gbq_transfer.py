from google.cloud import bigquery, storage
import pandas as pd
import os
import json

def df_to_gbq(df, dataset, table_name):
    bq_client = bigquery.Client(project="modalitydashboards")
    table_id = f"modalitydashboards.{dataset}.{table_name}"
    
    schema = [
        bigquery.SchemaField("Year", "STRING"),
        bigquery.SchemaField("Month", "STRING"),
        bigquery.SchemaField("MonthNumeric", "STRING"),
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Account", "STRING"),
        bigquery.SchemaField("A_C_Ref", "STRING"),
        bigquery.SchemaField("CAT", "STRING"),
        bigquery.SchemaField("Reporting_Code", "STRING"),
        bigquery.SchemaField("Reporting_Description", "STRING"),
        bigquery.SchemaField("CC", "STRING"),
        bigquery.SchemaField("Dp", "STRING"),
        bigquery.SchemaField("YTD", "FLOAT"),
        bigquery.SchemaField("Income_Expenses", "STRING"),
        bigquery.SchemaField("List_Size", "INTEGER"),
        bigquery.SchemaField("Period_1000", "FLOAT"),
        bigquery.SchemaField("YTD_1000", "FLOAT"),
        bigquery.SchemaField("Practice_Weighted_List_Size", "FLOAT"),
        bigquery.SchemaField("Practice_Raw_List_Size", "FLOAT"),
        bigquery.SchemaField("Divisional_Weighted_List_Size", "FLOAT"),
        bigquery.SchemaField("Divisional_raw_List_Size", "FLOAT"),
        bigquery.SchemaField("YTD_practice_weighted1000", "FLOAT"),
        bigquery.SchemaField("YTD_practice_raw_1000", "FLOAT"),
        bigquery.SchemaField("YTD_Divisional_weighted_1000", "FLOAT"),
        bigquery.SchemaField("YTD_Divisional_raw_1000", "FLOAT")
    ]

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

def delete_blob(bucket_name, blob_name):
    """Deletes a blob (file) from a bucket."""
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print(f"{blob_name} deleted.")