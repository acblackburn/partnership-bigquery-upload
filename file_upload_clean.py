import pandas as pd
import numpy as np
import xlrd

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
    df = pd.read_csv(input_file, dtype = {'Year':'Int64'})

    # Parse 'Date' column from 'Month' and 'Year' columns
    df['Date'] = df['Month'].str.strip() + "/" + df['Year'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], format="%B/%Y")

    # Create a numerical month column from the above created date column
    df['MonthNumeric'] = df['Date'].dt.strftime("%m")

    required_columns = ['Year', 'Month', 'MonthNumeric', 'Date', 'Account', 'A/C Ref', 'CAT',
       'Reporting Code', 'Reporting Description', 'CC', 'Dp', 'YTD',
       'Income / Expenses', 'List Size', 'Period per 1000', 'YTD per 1000',
       'Practice Weighted List Size', 'Practice Raw List Size',
       'Divisional weighted List Size', 'Divisional raw List Size',
       'YTD/practice weighted1000', 'YTD/practice raw 1000',
       'YTD/Divisional weighted 1000', 'YTD/Divisional raw 1000']

    for column in df.columns:
        if column not in required_columns:
              df.drop(column, axis=1)
    
    return df


