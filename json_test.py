import json

json_file = open("metadata.json")
data = json.load(json_file)
budget = data['budget']

for i in budget:
    print(i['csv_name'])