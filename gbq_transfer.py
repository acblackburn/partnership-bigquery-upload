from google.cloud import bigquery
import pandas as pd

def df_to_gbq(df, table_id):

    client = bigquery.Client()

    # Specify "object" datatypes as "STRING" for bq schema
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("contact_no", "STRING")
    ])

    # Load DataFrame in BigQuery
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for load job to complete
    job.result()






