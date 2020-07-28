import pandas as pd
import numpy as np
import xlrd
import json

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
        call_data['Call_Direction'] = call_info[8]
        call_data['Location'] = call_info[14]

        # Convert 'Start time' and 'Duration' columns to datetime formats
        call_data['Start time'] = pd.to_datetime(call_data['Start time'], format="%d/%m/%Y %H:%M:%S")
        call_data['Duration'] = pd.to_timedelta(call_data['Duration'])

        # Create a unique ID for each call
        call_data['Call_ID'] = call_data['Start time'].dt.strftime(f"%Y%m{call_no:04}")
        
        # Append each call DataFrame to the full DataFrame
        full_df = full_df.append(call_data)

        # Increment call counter
        call_no += 1

full_df.reset_index(drop=True, inplace=True)