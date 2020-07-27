import pandas as pd
import numpy as np
import xlrd
import json

df = pd.read_excel(
    "data/P641Lifecyclereport2020072318165587.xls",
    skiprows=7
)

df.dropna(axis=1, how='all', inplace=True)

print(df.head)