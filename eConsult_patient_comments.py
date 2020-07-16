import numpy as np
import pandas as pd
import xlrd
from datetime import datetime
from google.cloud import bigquery

patient_comments = pd.read_excel(
    "data/eConsult patient survey report for Modality - 20200601-20200630.xlsx",
    sheet_name='Patient comments',
    header=None
)

# Pull out month from the spreadsheet
workbook = xlrd.open_workbook("data/eConsult patient survey report for Modality - 20200601-20200630.xlsx")
worksheet = workbook.sheet_by_name('Patient feedback')
month_str = worksheet.cell(4, 1).value
month = datetime.strptime(month_str, "Reporting period: %d/%m/%Y - 30/06/2020").date()

class IndividualPracticeComments:
    
    def __init__(self, df):
        self.name = df.iloc[0][0]
        
        if len(df.index) > 0:
            df = df[1:]
            df.reset_index(drop=True, inplace=True)
            df = df.rename(columns={0:'Response', 1:'Comment'})
            self.data = df
        else:
            self.data = None

patient_comment_df_list = np.split(patient_comments, patient_comments[patient_comments.isnull().all(1)].index)

# Create empty dataframe to append each cleaned question to
full_df = pd.DataFrame()

for df in patient_comment_df_list:
    practice = IndividualPracticeComments(df)

    if practice.data is None:
        continue

    practice.data.insert(loc=0, column="Month", value=month)
    practice.data.insert(loc=1, column="Practice", value=practice.name)

    print(practice.data)
    full_df = full_df.append(practice.data)

# print(full_df)