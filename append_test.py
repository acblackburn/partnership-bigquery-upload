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

# Identify dataframe columns of a object (string) datatype
df_dtypes = new_df.dtypes
df_strings = df_dtypes[df_dtypes == 'object']
df_strings = df_strings.index.tolist()

# Specify bigquery "STRING" datatypes
for column in df_strings:
    schema.append(bigquery.SchemaField(column, "STRING"))
job_config = bigquery.LoadJobConfig(schema=schema)

dataset_ref = bigquery.dataset.DatasetReference("example-data-pipeline","gbq-test")
table_ref = bigquery.table.TableReference(dataset_ref, "dashboard_test")
table = bigquery.table.Table(table_ref)

# Load DataFrame into BigQuery
job = client.insert_rows_from_dataframe(table, new_df)

# Wait for load job to complete
job.result()