import pandas as pd
import numpy as np
import xlrd
import json

raw_df = pd.read_excel(
    "data/P641Lifecyclereport2020072318165587.xls",
    skiprows=7
)

raw_df.dropna(axis=1, how='all', inplace=True)

df_list = np.split(raw_df, raw_df[raw_df.iloc[:, 0] == "Time:"].index)

full_df = pd.DataFrame()

call_no = 1

for df in df_list:
    
    if not df.empty:
        call_info = df.iloc[0]
        call_data = df.iloc[1:]
        call_data = call_data.dropna(axis=1, how='all')
        call_data = call_data.dropna(axis=0, how='all')
        header = call_data.iloc[0]
        call_data = call_data.iloc[1:]
        call_data.columns = header
        
        call_data.columns.name = None
        
        call_data['Call_Direction'] = call_info[8]
        call_data['Location'] = call_info[14]

        call_data['Start time'] = pd.to_datetime(call_data['Start time'], format="%d/%m/%Y %H:%M:%S")
        call_data['Duration'] = pd.to_timedelta(call_data['Duration'])

        call_data['Call_ID'] = call_data['Start time'].dt.strftime(f"%Y%m{call_no:04}")
        
        full_df = full_df.append(call_data)

        call_no += 1

full_df.reset_index(drop=True, inplace=True)
print(full_df)