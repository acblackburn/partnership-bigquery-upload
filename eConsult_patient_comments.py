import json
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

# Open practice metadata file
with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)

# Pull out month from the spreadsheet
workbook = xlrd.open_workbook("data/eConsult patient survey report for Modality - 20200601-20200630.xlsx")
worksheet = workbook.sheet_by_name('Patient feedback')
month_str = worksheet.cell(4, 1).value
month = datetime.strptime(month_str, "Reporting period: %d/%m/%Y - 30/06/2020").date()

class IndividualPracticeComments:
    
    def __init__(self, df):
        
        df.dropna(axis=0, how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        
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

    practice.data["Month"] = month
    practice.data["Practice"] = practice.name
    practice.data['DIV'] = practice.data['Practice'].map({entry['practice_name']:entry['DIV'] for entry in practice_lookup})
    practice.data['Practice_Code'] = practice.data['Practice'].map({entry['practice_name']:entry['practice_code'] for entry in practice_lookup})

    full_df = full_df.append(practice.data)

# Reorder columns
full_df = full_df.reindex(
    columns=['Month', 'DIV', 'Practice_Code', 'Practice', 'Response', 'Comment']
)

full_df.reset_index(drop=True, inplace=True)

bq_client = bigquery.Client(project="modalitydashboards")
table_id = "modalitydashboards.eConsult.patient_feedback_comments"

schema = [
    bigquery.SchemaField('Month', 'DATE'),
    bigquery.SchemaField('DIV', 'STRING'),
    bigquery.SchemaField('Practice_Code', 'STRING'),
    bigquery.SchemaField('Practice', 'STRING'),
    bigquery.SchemaField('Response', 'STRING'),
    bigquery.SchemaField('Comment', 'STRING'),
]

job_config = bigquery.LoadJobConfig(schema=schema)

# Load DataFrame in BigQuery
job = bq_client.load_table_from_dataframe(
    full_df, table_id, job_config=job_config
)

# Wait for load job to complete
job.result()