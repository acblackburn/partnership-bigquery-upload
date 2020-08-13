import pandas as pd
import numpy as np
import xlrd
from datetime import datetime

# Open and load json metadata file
with open("metadata.json") as json_file:
    data = json.load(json_file)
    budget_metadata = data['Budget']

df = pd.read_csv("~/Desktop/Budgetv1.csv")

print(df.dtypes)
