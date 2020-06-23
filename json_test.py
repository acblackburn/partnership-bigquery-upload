import json
from google.cloud import bigquery

json_file = open("metadata.json")
data = json.load(json_file)
budget = data['budget']

required_columns = [entry['csv_name'] for entry in budget if entry['csv_name'] != None]

# required_columns = ['Year', 'Month', 'MonthNumeric', 'Date', 'Account', 'A/C Ref', 'CAT',
# 'Reporting Code', 'Reporting Description', 'CC', 'Dp', 'YTD',
# 'Income / Expenses', 'List Size', 'Period per 1000', 'YTD per 1000',
# 'Practice Weighted List Size', 'Practice Raw List Size',
# 'Divisional weighted List Size', 'Divisional raw List Size',
# 'YTD/practice weighted1000', 'YTD/practice raw 1000',
# 'YTD/Divisional weighted 1000', 'YTD/Divisional raw 1000']

# TODO redo this section using json file
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

# print(required_columns)
schema = [bigquery.SchemaField(entry['bq_name'], entry['bq_dtype']) for entry in budget]

print(schema)