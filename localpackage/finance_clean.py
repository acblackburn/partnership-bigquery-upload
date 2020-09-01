import pandas as pd
import numpy as np
import xlrd
import json
from datetime import datetime

def clean_budget(input_file):
    """Cleans monthly Budget csv file."""

    # Open and load json metadata file
    with open("metadata.json") as json_file:
        data = json.load(json_file)
        budget_metadata = data['Budget']

    # Required columns to read into df
    required_input_cols = [entry['csv_name'] for entry in budget_metadata if entry['csv_name'] != None]

    # Create dictionary for the panda type that the excel/csv file should be loaded as
    pd_dtypes = {entry['csv_name']:entry['pd_dtype'] for entry in budget_metadata if entry['csv_name'] != None}

    df = pd.read_excel(
        input_file, usecols=required_input_cols, dtype=pd_dtypes, na_values=' -   ', thousands=','
        )

    # Remove empty rows
    df.dropna(axis=0, how='all', inplace=True)

    # Parse 'Date' column from 'Month' and 'Year' columns
    df['Date'] = df['Month'].str.strip() + "/" + df['Year'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], format="%B/%Y")

    # Create a numerical month column from the above created date column
    df['Month_Numeric'] = df['Date'].dt.strftime("%m")

    # Make sure Division and CC columns are in uppercase format
    df['Dp'] = df['Dp'].str.upper()
    df['CC'] = df['CC'].str.upper()

    # Read in relevant list size data
    list_size_table = pd.read_excel(input_file, sheet_name=1, skiprows=4, usecols=[3,4,6,7])

    # Strip whitespace from list size column headers
    list_size_table.columns = list_size_table.columns.str.strip()

    # Split each patient feedback question into individual DataFrames
    list_size_table = np.split(list_size_table, list_size_table[list_size_table.isnull().all(1)].index)[0]

    # Make sure Division and Cost code columns are uppercase
    list_size_table['Div'] = list_size_table['Div'].str.upper()
    list_size_table['Cost code'] = list_size_table['Cost code'].str.upper()

    # Create a list of unique divisions
    divisions = list_size_table['Div'].unique()

    div_list_size_weighted = {}
    div_list_size_raw = {}

    for index,row in list_size_table.iterrows():
        division = row[1]
        weighted = row[2]
        raw = row[3]

        if pd.isnull(raw):
            continue

        if division in div_list_size_weighted:
            div_list_size_weighted[division] += weighted
            div_list_size_raw[division] += int(raw)
        else:
            div_list_size_weighted[division] = weighted
            div_list_size_raw[division] = int(raw)

    df['Divisional_Weighted_List_Size'] = df['Dp'].map(div_list_size_weighted)
    df['Divisional_Raw_List_Size'] = df['Dp'].map(div_list_size_raw)

    # For practice map all practices from the table
    prac_list_size_weighted = pd.Series(list_size_table['Weighted'].values, index=list_size_table['Cost code']).to_dict()
    prac_list_size_raw = pd.Series(list_size_table['Raw'].values, index=list_size_table['Cost code']).to_dict()

    # print(prac_list_size_weighted)

    df['Practice_Weighted_List_Size'] = df['CC'].map(prac_list_size_weighted, na_action=None)
    df['Practice_Raw_List_Size'] = df['CC'].map(prac_list_size_raw, na_action=None)

    # print(df['Practice_Raw_List_Size'].value_counts())

    # Create YTD/1000 columns
    df["YTD_Divisional_Raw_1000"] = 1000 * df['YTD'] / df['Divisional_Raw_List_Size']
    df["YTD_Divisional_Weighted_1000"] = 1000 * df['YTD'] / df['Divisional_Weighted_List_Size']
    df["YTD_Practice_Weighted_1000"] = 1000 * df['YTD'] / df['Practice_Weighted_List_Size']
    df["YTD_Practice_Raw_1000"] = 1000 * df['YTD'] / df['Practice_Raw_List_Size']

    # Create Period/1000 columns
    df["Period_Divisional_Raw_1000"] = 1000 * df['Period'] / df['Divisional_Raw_List_Size']
    df["Period_Divisional_Weighted_1000"] = 1000 * df['Period'] / df['Divisional_Weighted_List_Size']
    df["Period_Practice_Weighted_1000"] = 1000 * df['Period'] / df['Practice_Weighted_List_Size']
    df["Period_Practice_Raw_1000"] = 1000 * df['Period'] / df['Practice_Raw_List_Size']

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

    return df
