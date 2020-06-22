import json

json_file = open("budget_metadata.json")

data = json.load(json_file)

for i in data["budget"]:
    print(i)