import pandas as pd
import numpy as np
import xlrd

def clean_groupmetrics(input_file):
    df = pd.read_excel(input_file)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ","_")
    df['Metric'] = df['Metric'].str.replace(" ","_")
    df_new = df.pivot(index = 'YTD_Period', columns = 'Metric', values = 'Value')
    df_new['Profit_Per_WTE_Partner'].fillna(df_new['ProfitPer_WTE_Partner'], inplace = True)
    df_new.drop(columns = 'ProfitPer_WTE_Partner')
    return df_new

def month_numeric(str):
    if str == 'January':
        return 1
    if str == 'February':
        return 2
    if str == 'March':
        return 3
    if str == 'April':
        return 4
    if str == 'May':
        return 5
    if str == 'June':
        return 6
    if str == 'July':
        return 7
    if str == 'August':
        return 8 
    if str == 'September':
        return 9
    if str == 'October':
        return 10
    if str == 'November':
        return 11
    if str == 'December':
        return 12

def clean_budgetsheet(input_file):
    df_add = pd.read_excel(input_file)
    df_add['MonthNumeric'] = df_add['Month'].apply(month_numeric)
    date_string = df_add['MonthNumeric'].astype(str) + '/' + df_add['Year'].astype(str)
    datetime_string = pd.to_datetime(date_string, format='%m/%Y')

    df_add['Date'] = datetime_string
    columns = ['Year', 'Month', 'MonthNumeric', 'Date', 'Account', 'A/C Ref', 'CAT',
       'Reporting Code', 'Reporting Description', 'CC', 'Dp', 'YTD',
       'Income / Expenses', 'List Size', 'Period per 1000', 'YTD per 1000',
       'Practice Weighted List Size', 'Practice Raw List Size',
       'Divisional weighted List Size', 'Divisional raw List Size',
       'YTD/practice weighted1000', 'YTD/practice raw 1000',
       'YTD/Divisional weighted 1000', 'YTD/Divisional raw 1000']
    columns_add = df_add.columns
    for column in columns_add:
        if column not in columns_add:
            df_add.drop(column)
    return df_add
    


