from google.cloud import bigquery
import pandas as pd

def df_to_gbq(df, table_name):
    
    client = bigquery.Client()
    table_id = "example-data-pipeline.gbq_test." + table_name
    schema = []

    # Identify dataframe columns of a object (string) datatype
    df_dtypes = df.dtypes
    for column, dtype in df_dtypes.items():
        if dtype == "object":
            schema.append(bigquery.SchemaField(column, "STRING"))
    job_config = bigquery.LoadJobConfig(schema=schema)

    # Load DataFrame in BigQuery
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for load job to complete
    job.result()






