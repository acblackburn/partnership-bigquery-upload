from google.cloud import bigquery
import pandas as pd

# Test DataFrame
new_df = pd.DataFrame({
    "personID": [5],
    "first_name": ["NewGuy"],
    "last_name": ["FromANewDF"],
    "contact_no": ["000005"]
})

client = bigquery.Client()
table_id = "example-data-pipeline.gbq_test.dashboard_test"
schema = []

# Create Series of columns and datatypes.
df_dtypes = new_df.dtypes

# Specify bigquery table schema
for column, dtype in df_dtypes.items():
    if dtype == "object":
        schema.append(bigquery.SchemaField(column, "STRING"))
    elif dtype == "int64":
        schema.append(bigquery.SchemaField(column, "INTEGER"))
    elif dtype == "float64":
        schema.append(bigquery.SchemaField(column, "FLOAT"))
job_config = bigquery.LoadJobConfig(schema=schema)

#dataset_ref = bigquery.dataset.DatasetReference("example-data-pipeline","gbq_test")
#table_ref = bigquery.table.TableReference(dataset_ref, "dashboard_test")
#table = bigquery.table.Table(table_ref, schema)

# Load DataFrame into BigQuery
job = client.load_table_from_dataframe(new_df, table_id, job_config=job_config)

job.result()
