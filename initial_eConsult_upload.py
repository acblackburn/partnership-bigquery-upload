import pandas as pd
import numpy as np
import xlrd
import json
from datetime import datetime
from google.cloud import bigquery

def initial_reason_upload(input_file):
    df = pd.read_excel(input_file, dtype={"Time":"str"})

    df['List_Size'] = df['List_Size'].round().astype(int)
    df['Div_List'] = df['Div_List'].round().astype(int)
    df['Month'] = df['Date'].dt.strftime('%B')
    df['Diverted'] = df['Diverted'].fillna('N')

    bq_client = bigquery.Client(project="modalitydashboards")
    table_id = "modalitydashboards.eConsult.Reason"

    json_file = open("metadata.json")
    data = json.load(json_file)
    metadata = data['Reason']

    # Create BigQuery schema from json metadata 
    schema = [bigquery.SchemaField(entry['bq_name'], entry['bq_dtype']) for entry in metadata if entry['bq_name'] != None]

    job_config = bigquery.LoadJobConfig(schema=schema)

    # Load DataFrame in BigQuery
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    job.result()
    json_file.close()

def initial_usage_upload(input_file):
    df = pd.read_excel(input_file)
    
    df['List_Size'] = df['List_Size'].round().astype(int)

    bq_client = bigquery.Client(project="modalitydashboards")
    table_id = "modalitydashboards.eConsult.Usage"

    json_file = open("metadata.json")
    data = json.load(json_file)
    metadata = data['Usage']

    # Create BigQuery schema from json metadata 
    schema = [bigquery.SchemaField(entry['bq_name'], entry['bq_dtype']) for entry in metadata if entry['bq_name'] != None]

    job_config = bigquery.LoadJobConfig(schema=schema)

    # Load DataFrame in BigQuery
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    job.result()
    json_file.close()

initial_usage_upload('data/econsult_usage.xlsx')
initial_reason_upload('data/econsult_reason.xlsx')
