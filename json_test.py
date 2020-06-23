import json

json_file = open("metadata.json")
data = json.load(json_file)
budget = data['budget']

required_columns = [entry['csv_name'] for entry in budget if entry['csv_name'] != None]

""" required_columns = ['Year', 'Month', 'MonthNumeric', 'Date', 'Account', 'A/C Ref', 'CAT',
'Reporting Code', 'Reporting Description', 'CC', 'Dp', 'YTD',
'Income / Expenses', 'List Size', 'Period per 1000', 'YTD per 1000',
'Practice Weighted List Size', 'Practice Raw List Size',
'Divisional weighted List Size', 'Divisional raw List Size',
'YTD/practice weighted1000', 'YTD/practice raw 1000',
'YTD/Divisional weighted 1000', 'YTD/Divisional raw 1000'] """

print(required_columns)