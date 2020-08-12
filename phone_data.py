import pandas as pd
import numpy as np
import xlrd
import json
from google.cloud import bigquery

# Open raw excel data
raw_df = pd.read_excel(
    "data/P641Lifecyclereport2020072318165587.xls",
    skiprows=7
)

# Drop empty columns
raw_df.dropna(axis=1, how='all', inplace=True)

# Split the raw data into a list of DataFrames based on call
df_list = np.split(raw_df, raw_df[raw_df.iloc[:, 0] == "Time:"].index)

# Create an empty dataframe to append clean data to
full_df = pd.DataFrame()

# Initialise the call counter from 1
call_no = 1

for df in df_list:
    # For each call in df list, clean up
    if not df.empty:
        # Top row contains call metadata
        call_info = df.iloc[0]
        call_data = df.iloc[1:]

        # Drop empty rows/columns from the call data
        call_data = call_data.dropna(axis=1, how='all')
        call_data = call_data.dropna(axis=0, how='all')

        # Reassign the header row
        header = call_data.iloc[0]
        call_data = call_data.iloc[1:]
        call_data.columns = header
        
        # Remove index name
        call_data.columns.name = None
        
        # Add 'Call_Direction' and 'Location' columns
        call_data['Call_direction'] = call_info[8]
        call_data['Location'] = call_info[14]

        # Convert 'Start time' column to datetime format
        call_data['Start time'] = pd.to_datetime(call_data['Start time'], format="%d/%m/%Y %H:%M:%S")

        # Create a unique ID for each call
        call_data['Call_ID'] = call_data['Start time'].dt.strftime(f"%Y%m{call_no:04}")

        new_row = {
            'Start time': [call_data.iloc[0, 0]],
            'Event type': ['Call initiated'],
            'Duration': ['00:00:00'],
            'Call_direction': [call_data.iloc[0, 7]],
            'Location': [call_data.iloc[0, 8]],
            'Call_ID': [call_data.iloc[0, 9]]
            }

        call_data = pd.concat([pd.DataFrame(new_row), call_data], ignore_index=True)
        
        # Append each call DataFrame to the full DataFrame
        full_df = full_df.append(call_data)

        # Increment call counter
        call_no += 1

full_df.reset_index(drop=True, inplace=True)

full_df.columns = full_df.columns.str.replace(' ', '_')


# # full_df.to_csv("~/Desktop/phone_data_clean.csv", index=False)

# bq_client = bigquery.Client(project="modalitydashboards")
# table_id = f"modalitydashboards.phone_data.example_month"

# schema = [
#     bigquery.SchemaField("Start_time", "TIMESTAMP"),
#     bigquery.SchemaField("Duration", "STRING"),
#     bigquery.SchemaField("Event_type", "STRING"),
#     bigquery.SchemaField("Device_type", "STRING"),
#     bigquery.SchemaField("Reporting", "STRING"),
#     bigquery.SchemaField("Full_name", "STRING"),
#     bigquery.SchemaField("Comment", "STRING"),
#     bigquery.SchemaField("Call_direction", "STRING"),
#     bigquery.SchemaField("Location", "STRING"),
#     bigquery.SchemaField("Call_ID", "STRING")
# ]

# job_config = bigquery.LoadJobConfig(schema=schema)

# # Load DataFrame in BigQuery
# job = bq_client.load_table_from_dataframe(
#     full_df, table_id, job_config=job_config
# )

# # Wait for load job to complete
# job.result()