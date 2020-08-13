import pandas as pd
import numpy as np
import xlrd
import json
from datetime import datetime

# Open and load json metadata file
with open("metadata.json") as json_file:
    data = json.load(json_file)
    budget_metadata = data['Budget']

df = pd.read_csv("~/Desktop/Budgetv1.csv")

# Remove empty rows
df.dropna(axis=0, how='all', inplace=True)

df['Year'] = df['Year'].astype(str)
df['CAT'] = df['CAT'].astype(str)
df['Reporting_Code'] = df['Reporting_Code'].astype(str)

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

print(df.dtypes)
