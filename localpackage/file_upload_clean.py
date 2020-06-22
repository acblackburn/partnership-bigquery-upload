import pandas as pd
import numpy as np
import xlrd
import json

def clean_groupmetrics(input_file):
    df = pd.read_csv(input_file)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ","_")
    df['Metric'] = df['Metric'].str.replace(" ","_")
    df_new = df.pivot(index = 'YTD_Period', columns = 'Metric', values = 'Value')
    df_new['Profit_Per_WTE_Partner'].fillna(df_new['ProfitPer_WTE_Partner'], inplace = True)
    df_new.drop(columns = 'ProfitPer_WTE_Partner')
    return df_new

def clean_budget(input_file):
    """Cleans monthly Budget csv file to specifications."""
    # Open and load json metadata file
    json_file = open("../metadata.json")
    data = json.load(json_file)
    budget_metadata = data['budget']
    
    pd_dtypes = {}
    for entry in budget_metadata:
        if entry['csv_name'] != None:
            pd_dtypes[entry['csv_name']] = entry['pd_dtype']
    
    df = pd.read_csv(input_file, dtype=pd_dtypes, na_values=' -   ', thousands=',')
    
    # Parse 'Date' column from 'Month' and 'Year' columns
    df['Date'] = df['Month'].str.strip() + "/" + df['Year'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], format="%B/%Y")

    # Create a numerical month column from the above created date column
    df['MonthNumeric'] = df['Date'].dt.strftime("%m")

    # Remove leading and trailing spaces from column headers
    df.columns = df.columns.str.strip()

    # Make sure Division and CC columns are in uppercase format
    df['Dp'] = df['Dp'].str.upper()
    df['CC'] = df['CC'].str.upper()

    required_columns = ['Year', 'Month', 'MonthNumeric', 'Date', 'Account', 'A/C Ref', 'CAT',
       'Reporting Code', 'Reporting Description', 'CC', 'Dp', 'YTD',
       'Income / Expenses', 'List Size', 'Period per 1000', 'YTD per 1000',
       'Practice Weighted List Size', 'Practice Raw List Size',
       'Divisional weighted List Size', 'Divisional raw List Size',
       'YTD/practice weighted1000', 'YTD/practice raw 1000',
       'YTD/Divisional weighted 1000', 'YTD/Divisional raw 1000']
    
    for column in df.columns:
        if column not in required_columns:
              df = df.drop(column, axis=1)

    df.rename(columns={
        'Income / Expenses':'Income_Expenses',
        'Period per 1000':'Period_1000',
        'YTD per 1000':'YTD_1000',
        'Divisional weighted List Size':'Divisional_Weighted_List_Size',
        'Divisional raw List Size':'Divisional_raw_List_Size',
        'YTD/practice weighted1000':'YTD_practice_weighted1000',
        'YTD/practice raw 1000':'YTD_practice_raw_1000',
        'YTD/Divisional weighted 1000':'YTD_Divisional_weighted_1000',
        'YTD/Divisional raw 1000':'YTD_Divisional_raw_1000'
    }, inplace=True)

    df.columns = df.columns.str.replace(' ','_').str.replace('/','_')
    df = df.sort_values("Date")

    json_file.close()
    
    return df