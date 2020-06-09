import pandas as pd
import numpy as np

def clean(input_file):
    df = pd.read_excel(input_file)
    df_new = df.pivot(index = 'YTD Period', columns = 'Metric', values = 'Value')
    df_new['Profit Per WTE Partner'].fillna(df_new['ProfitPer WTE Partner'], inplace = True)
    df_new.drop(columns = 'ProfitPer WTE Partner')
    return df_new