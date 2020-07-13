import xlrd
import json
import numpy as np
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

class FeedbackQuestion:
    
    def __init__(self, df):
        self.question = df.iloc[0][0]
        
        if len(df.index) > 1:
            header = df.iloc[1]
            df = df[2:]
            df.columns = header
            df.reset_index(drop=True, inplace=True)
            df.columns.name = None
            df.set_index('PRACTICE', inplace=True)
            self.data = df
        else:
            self.data = None

    def stack_responses(self):
        """Converts responses from questions in columns to responses in individual rows"""
        self.data = self.data.stack().to_frame().reset_index()
        self.data = self.data.rename(
            columns={'PRACTICE':'Practice', 'level_1':'Response', 0:'Number_of_Responses'}
        )

    def remove_grandtotal(self):
        """Removes Grand Total rows from each question sub-dataframe"""
        self.data = self.data[self.data.Practice != "Grand Total"]

# Open practice metadata file
with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)

patient_feedback = pd.read_excel(
    "data/eConsult patient survey report for Modality - 20200601-20200630.xlsx",
    sheet_name="Patient feedback",
    header=None,
    skiprows=16
)

patient_feedback.dropna(axis=1, how='all', inplace=True)

# Pull out month from the spreadsheet
workbook = xlrd.open_workbook("data/eConsult patient survey report for Modality - 20200601-20200630.xlsx")
worksheet = workbook.sheet_by_name('Patient feedback')
month_str = worksheet.cell(4, 1).value
month = datetime.strptime(month_str, "Reporting period: %d/%m/%Y - 30/06/2020").date()

# Split each patient feedback question into individual dataframes
patient_feedback_df_list = np.split(patient_feedback, patient_feedback[patient_feedback.isnull().all(1)].index)

# Create empty dataframe to append each cleaned question to
full_df = pd.DataFrame()

for df in patient_feedback_df_list:
    df.dropna(axis=0, how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)

    # Create question instance
    question = FeedbackQuestion(df)
    
    # Skip current iteration of loop if there is no data
    if question.data is None:
        continue

    # Stack question responses into rows  
    question.stack_responses()

    # Remove 'Grand Total' row from each question
    question.remove_grandtotal()

    # Add Question, Division and Practice code columns to each data frame
    question.data['Question'] = question.question
    question.data['DIV'] = question.data['Practice'].map({entry['practice_name']:entry['DIV'] for entry in practice_lookup})
    question.data['Practice_Code'] = question.data['Practice'].map({entry['practice_name']:entry['practice_code'] for entry in practice_lookup})
    # question.data['Response_Index'] = np.arange(len(question.data))

    # Append each question to the combined data frame.
    full_df = full_df.append(question.data)

full_df['Response_Index'] = np.arange(len(full_df))

# Add Month column
full_df['Month'] = month

# Reorder columns
full_df = full_df.reindex(
    columns=['Month', 'DIV', 'Practice_Code', 'Practice', 'Question', 'Response', 'Number_of_Responses', 'Response_Index']
)

full_df['Number_of_Responses'] = full_df['Number_of_Responses'].astype(int)

bq_client = bigquery.Client(project="modalitydashboards")
table_id = "modalitydashboards.eConsult.patient_feedback_response"

schema = [
    bigquery.SchemaField('Month', 'DATE'),
    bigquery.SchemaField('DIV', 'STRING'),
    bigquery.SchemaField('Practice_Code', 'STRING'),
    bigquery.SchemaField('Practice', 'STRING'),
    bigquery.SchemaField('Question', 'STRING'),
    bigquery.SchemaField('Response', 'STRING'),
    bigquery.SchemaField('Number_of_Responses', 'INTEGER'),
    bigquery.SchemaField('Response_Index', 'INTEGER')
]

job_config = bigquery.LoadJobConfig(schema=schema)

# Load DataFrame in BigQuery
job = bq_client.load_table_from_dataframe(
    full_df, table_id, job_config=job_config
)

# Wait for load job to complete
job.result()