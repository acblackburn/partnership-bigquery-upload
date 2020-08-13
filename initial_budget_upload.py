import pandas as pd
import numpy as np
import xlrd
import json
from datetime import datetime
from google.cloud import bigquery

# Open and load json metadata file
with open("metadata.json") as json_file:
    data = json.load(json_file)
    budget_metadata = data['Budget']

# Create dictionary for the panda type that the excel/csv file should be loaded as
pd_dtypes = {entry['csv_name']:entry['pd_dtype'] for entry in budget_metadata if entry['csv_name'] != None}


df = pd.read_excel("~/Desktop/Budgetv1.xlsx", dtype=pd_dtypes, na_values=' -   ', thousands=',')

# Remove empty rows
df.dropna(axis=0, how='all', inplace=True)

# df['Year'] = df['Year'].astype(str)
# df['CAT'] = df['CAT'].astype(str)
# df['Reporting_Code'] = df['Reporting_Code'].astype(str)

missing_period_cols = [
        'Period',
        'Period_Practice_Weighted_1000',
        'Period_Practice_Raw_1000',
        'Period_Divisional_Weighted_1000',
        'Period_Divisional_Raw_1000'
    ]

for col in missing_period_cols:
    df[col] = None
    df[col] = df[col].astype('Float64')

# Parse 'Date' column from 'Month' and 'Year' columns
df['Date'] = df['Month'].str.strip() + "/" + df['Year'].astype(str)
df['Date'] = pd.to_datetime(df['Date'], format="%B/%Y")

# Create a numerical month column from the above created date column
df['Month_Numeric'] = df['Date'].dt.strftime("%m")

# Make sure Division and CC columns are in uppercase format
df['Dp'] = df['Dp'].str.upper()
df['CC'] = df['CC'].str.upper()

# Rename columns from csv_name to bq_name
columns_rename = {entry['csv_name']:entry['bq_name'] for entry in budget_metadata}
df.rename(columns=columns_rename, inplace=True)

# Create list of required columns that appear in metadata.json
required_columns = [entry['bq_name'] for entry in budget_metadata]

for column in df.columns:
    if column not in required_columns:
          df = df.drop(column, axis=1)

# Sort DataFrame
df = df.sort_values('Date', ignore_index=True)

print(df.dtypes)

bq_client = bigquery.Client(project="modalitydashboards")
table_id = f"modalitydashboards.Finance.Budget2"

# Create BigQuery schema from json metadata
schema = [bigquery.SchemaField(entry['bq_name'], entry['bq_dtype']) for entry in budget_metadata if entry['bq_name'] != None]

job_config = bigquery.LoadJobConfig(schema=schema)

# Load DataFrame in BigQuery
job = bq_client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)

# Wait for load job to complete
job.result()
