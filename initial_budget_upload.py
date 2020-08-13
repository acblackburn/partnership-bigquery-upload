import pandas as pd
import numpy as np
import xlrd
import json
from datetime import datetime

# Open and load json metadata file
with open("metadata.json") as json_file:
    data = json.load(json_file)
    budget_metadata = data['Budget']

df = pd.read_csv("~/Desktop/Budgetv1.csv")

df['Period'] = None
df['Period_Practice_Weighted_1000'] = None
df['Period_Practice_Raw_1000'] = None
df['Period_Divisional_Weighted_1000'] = None
df['Period_Divisional_Raw_1000'] = None

print(df.dtypes)
