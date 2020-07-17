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
    
    # Create dictionary for the panda type that the excel/csv file should be loaded as
    pd_dtypes = {entry['csv_name']:entry['pd_dtype'] for entry in budget_metadata if entry['csv_name'] != None}
    
    df = pd.read_excel(
        input_file, dtype=pd_dtypes, na_values=' -   ', thousands=','
        )
    
    # Remove empty rows and columns
    empty_columns = [col for col in df if col.startswith('Unnamed')]
    df.drop(empty_columns, axis=1, inplace=True)
    df.dropna(axis=0, how='all', inplace=True)

    # Parse 'Date' column from 'Month' and 'Year' columns
    df['Date'] = df['Month'].str.strip() + "/" + df['Year'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], format="%B/%Y")

    # Create a numerical month column from the above created date column
    df['MonthNumeric'] = df['Date'].dt.strftime("%m")

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
    
    return df
          
def clean_consultations(input_file):
    """Cleans eConsult data. File to be uploaded weekly."""

    # Open and load json metadata file
    with open("metadata.json") as json_file:
        data = json.load(json_file)
        usage_metadata = data['Usage']
        reason_metadata = data['Reason']
    
    with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)

    # Create dictionary for the panda type that the excel/csv file should be loaded as
    pd_dtypes_usage = {entry['csv_name']:entry['pd_dtype'] for entry in usage_metadata if entry['csv_name'] != None}
    pd_dtypes_reason = {entry['csv_name']:entry['pd_dtype'] for entry in reason_metadata if entry['csv_name'] != None}
    
    # Read in each excel sheet to separate DataFrames from the eConsult file
    usage_df = pd.read_excel(input_file, sheet_name='Usage', skiprows=21, dtype=pd_dtypes_usage)
    reason_df = pd.read_excel(input_file, sheet_name='All Consults', dtype=pd_dtypes_reason)

    # Read in ODS code, and List Size columns from NHSE
    e_consult_nhse = pd.read_excel(input_file, sheet_name='NHSE', usecols =["ODS Code","List Size"], dtype={'List Size':"Int64"})

    # Remove empty rows from both DataFrames
    usage_df.dropna(axis=0, how='all', inplace=True)
    reason_df.dropna(axis=0, how='all', inplace=True)

    # Look up table for list size created from NHSE DataFrame
    list_size_lookup = {row[0]:row[1] for index,row in e_consult_nhse.iterrows()}

    # Create set of unique Divisions
    unique_divisions = set(entry['DIV'] for entry in practice_lookup)

    divisional_list_size_lookup = {}
    for division in unique_divisions:
        for entry in practice_lookup:
            if entry['DIV'] == division: 
                if division not in divisional_list_size_lookup:
                    divisional_list_size_lookup[division] = list_size_lookup[entry['ODS Code']]
                else: 
                    divisional_list_size_lookup[division] += list_size_lookup[entry['ODS Code']]
    
    # Add Division and Practice Code columns to Reason DataFrame
    reason_df['DIV'] = reason_df['ODS Code'].map({entry['ODS Code']:entry['DIV'] for entry in practice_lookup})
    reason_df['Code'] = reason_df['ODS Code'].map({entry['ODS Code']:entry['practice_code'] for entry in practice_lookup})

    # Add practice and divisional list size column to reason dataframe
    reason_df['List_Size'] = reason_df['ODS Code'].apply(lambda x: list_size_lookup[x])
    reason_df['Div_List'] = reason_df['DIV'].apply(lambda x: divisional_list_size_lookup[x])

    # Add age bracket column to reason dataframe
    reason_df['Age_Bracket'] = reason_df['Age'].apply(age_bracket)
    
    # Reason per 1000 eConsults per divisional and practice list size
    reason_df['eConsult_1000_division'] = 1000 / reason_df['Div_List']
    reason_df['eConsult_1000_practice'] = 1000 / reason_df['List_Size']

    # Add singular month to reason dataframe
    reason_df['Month'] = reason_df['Date'].apply(lambda x: x.strftime("%B"))

    # Fill Reason Diverted column null values with No (N)
    reason_df['Diverted'] = reason_df['Diverted'].fillna("N")

    # Add Date and Time columns to Reason DataFrame cleaned to specified format
    reason_df['Time'] = reason_df['Time'].apply(lambda x: x.strftime('%H:%M:%S'))
    reason_df['Date'] = reason_df['Date'].apply(lambda x: x.date())

    # Add Division and Practice Code to Usage DataFrame
    usage_df['DIV'] = usage_df['ODS Code'].map({entry['ODS Code']:entry['DIV'] for entry in practice_lookup})
    usage_df['Code'] = usage_df['ODS Code'].map({entry['ODS Code']:entry['practice_code'] for entry in practice_lookup})

    # Add List Size column to Usage DataFrame
    usage_df['List_Size'] = reason_df['ODS Code'].apply(lambda x: list_size_lookup[x])

    # eConsults submitted per 1000 per practice List Size
    usage_df['eConsults_submitted_1000'] =(usage_df['eConsults submitted']/usage_df['List_Size'])*1000
    
    # Create Week_Month column in Date format
    usage_df['Week_Month'] = reason_df['Date'][0]

    # Add EMIS or S1 column to Usage DataFrame
    usage_df['EMIS_S1'] = usage_df['ODS Code'].map({entry['ODS Code']:entry['EMIS/S1'] for entry in practice_lookup})

    # Rename columns for BigQuery
    usage_columns_rename = {entry['csv_name']:entry['bq_name'] for entry in reason_metadata}
    reason_df.rename(columns=usage_columns_rename, inplace=True)
    reason_columns_rename = {entry['csv_name']:entry['bq_name'] for entry in usage_metadata}
    usage_df.rename(columns=reason_columns_rename, inplace=True)

    # Drop columns not in BQ schema
    required_usage_cols = [entry["bq_name"] for entry in usage_metadata if entry["bq_name"] != None]
    required_reason_cols = [entry["bq_name"] for entry in reason_metadata if entry["bq_name"] != None]
    usage_df.drop(columns=[col for col in usage_df if col not in required_usage_cols], inplace=True)
    reason_df.drop(columns=[col for col in reason_df if col not in required_reason_cols], inplace=True)

    return usage_df, reason_df

def age_bracket(age):
    if age < 10:
        return "0-10"
    elif age <= 20:
        return "11-20"
    elif age <= 30:
        return "21-30"
    elif age <=40:
        return "31-40"
    elif age <=50:
        return "41-50"
    elif age <= 60:
        return "51-60"
    elif age <= 70:
        return "61-70"
    else:
        return "70+"