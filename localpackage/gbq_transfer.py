from google.cloud import bigquery
import pandas as pd
import os

def df_to_gbq(df, dataset, table_name):
    bq_client = bigquery.Client(project="modalitydashboards")
    table_id = f"modalitydashboards.{dataset}.{table_name}"
    
    schema = [
        bigquery.SchemaField("Year", "STRING"),
        bigquery.SchemaField("Month", "STRING"),
        bigquery.SchemaField("MonthNumeric", "STRING"),
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Account", "STRING"),
        bigquery.SchemaField("A_C_Ref", "STRING"),
        bigquery.SchemaField("CAT", "STRING"),
        bigquery.SchemaField("Reporting_Code", "STRING"),
        bigquery.SchemaField("Reporting_Description", "STRING"),
        bigquery.SchemaField("CC", "STRING"),
        bigquery.SchemaField("Dp", "STRING"),
        bigquery.SchemaField("YTD", "FLOAT"),
        bigquery.SchemaField("Income_Expenses", "STRING"),
        bigquery.SchemaField("List_Size", "INTEGER"),
        bigquery.SchemaField("Period_1000", "FLOAT"),
        bigquery.SchemaField("YTD_1000", "FLOAT"),
        bigquery.SchemaField("Practice_Weighted_List_Size", "FLOAT"),
        bigquery.SchemaField("Practice_Raw_List_Size", "FLOAT"),
        bigquery.SchemaField("Divisional_Weighted_List_Size", "FLOAT"),
        bigquery.SchemaField("Divisional_raw_List_Size", "FLOAT"),
        bigquery.SchemaField("YTD_practice_weighted1000", "FLOAT"),
        bigquery.SchemaField("YTD_practice_raw_1000", "FLOAT"),
        bigquery.SchemaField("YTD_Divisional_weighted_1000", "FLOAT"),
        bigquery.SchemaField("YTD_Divisional_raw_1000", "FLOAT")
    ]

    # Identify dataframe columns of a object (string) datatype
    """ df_dtypes = df.dtypes
    for column, dtype in df_dtypes.items():
        if dtype == "object":
            schema.append(bigquery.SchemaField(column, "STRING"))
        elif dtype == "datetime64[ns]":
            schema.append(bigquery.SchemaField(column, "DATE")) """
    job_config = bigquery.LoadJobConfig(schema=schema)

    # Load DataFrame in BigQuery
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    # Wait for load job to complete
    job.result()

def event_file_info(string):
    path, filename = os.path.split(string)
    return (path, filename)