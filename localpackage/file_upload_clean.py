import pandas as pd
import numpy as np
import xlrd
import json
from datetime import datetime

def clean_budget(input_file):
    """Cleans monthly Budget csv file."""

    # Open and load json metadata file
    json_file = open("metadata.json")
    data = json.load(json_file)
    budget_metadata = data['Budget']
    
    # Create dictionary for the panda type that the excel/csv file should be loaded as
    pd_dtypes = {entry['csv_name']:entry['pd_dtype'] for entry in budget_metadata if entry['csv_name'] != None}
    
    df = pd.read_csv(
        input_file, dtype=pd_dtypes, na_values=' -   ', thousands=','
        )
    
    # Parse 'Date' column from 'Month' and 'Year' columns
    df['Date'] = df['Month'].str.strip() + "/" + df['Year'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], format="%B/%Y")

    # Create a numerical month column from the above created date column
    df['MonthNumeric'] = df['Date'].dt.strftime("%m")

    # Remove leading and trailing spaces from column headers
    df.columns = df.columns.str.strip()

    # Make sure Division and CC columns are in uppercase format
    df['Dp'] = df['Dp'].str.upper()
    df['CC'] = df['CC'].str.upper()

    # Rename columns from csv_name to bq_name
    columns_rename = {entry['csv_name']:entry['bq_name'] for entry in budget_metadata}
    df.rename(columns=columns_rename, inplace=True)

    # Create list of required columns that appear in metadata.json
    required_columns = [entry['bq_name'] for entry in budget_metadata]
    
    for column in df.columns:
        if column not in required_columns:
              df = df.drop(column, axis=1)

    # df.columns = df.columns.str.replace(' ','_').str.replace('/','_')
    df = df.sort_values('Date', ignore_index=True)
    json_file.close()
    
    return df

            
def age_bracket(int):
    if int <= 20:
        return "<20"
    elif int <= 30:
        return "21-30"
    elif int <=40: 
        return "31-40"
    elif int <=50:
        return "41-50"
    elif int <= 60:
        return "51-60"
    elif int <= 70:
        return "61-70"
    else: return "70+"

def consultations_clean(input_file):

    usage_df = pd.read_excel('econsult.xlsx', sheet_name='Usage', skiprows=21)
    reason_df = pd.read_excel('econsult.xlsx', sheet_name='All Consults')
    e_consult_nhse = pd.read_excel('econsult.xlsx', sheet_name='NHSE')

    #MAKE JSON FILE
    practice_lookuptable = {'M88013':{'div':'BIR','practice_code':'BCF'}, 
                        'M85124':{'div':'BIR','practice_code':'BEL'}, 
                        'G85124':{'div':'LEW','practice_code':'BGS'}, 
                        'H81037':{'div':'SUR','practice_code':'BMP'}, 
                        'K81638':{'div':'WOK','practice_code':'BUR'}, 
                        'H82040':{'div':'SUS','practice_code':'CHD'}, 
                        'B82020':{'div':'AWC','practice_code':'CRS'}, 
                        'M85178':{'div':'BIR','practice_code':'ENK'},
                        'B83021':{'div':'AWC','practice_code':'FAR'},
                        'B82028':{'div':'AWC','practice_code':'FIS'},
                        'M85002':{'div':'BIR','practice_code':'HWM'},
                        'B83027':{'div':'AWC','practice_code':'HAW'},
                        'M85016':{'div':'BIR','practice_code':'HIL'},
                        'B83023':{'div':'AWC','practice_code':'HOL'},
                        'G85004':{'div':'LEW','practice_code':'JEN'},
                        'H82010':{'div':'SUS','practice_code':'JCS'},
                        'B83033':{'div':'AWC','practice_code':'KIL'},
                        'M85069':{'div':'BIR','practice_code':'LPH'},
                        'M91028':{'div':'WAL','practice_code':'BER'},
                        'M91032':{'div':'WAL','practice_code':'COL'},
                        'M91660':{'div':'WAL','practice_code':'VAI'},
                        'M91613':{'div':'WAL','practice_code':'FOR'},
                        'Y02627':{'div':'WAL','practice_code':'HAR'},
                        'B81048':{'div':'HUL','practice_code':'NEL'},
                        'M91016':{'div':'WAL','practice_code':'NIN'},
                        'B83061':{'div':'AWC','practice_code':'OAK'},
                        'H82621':{'div':'SUS','practice_code':'PVH'},
                        'M91616':{'div':'WAL','practice_code':'PLE'},
                        'H82008':{'div':'SUS','practice_code':'SHI'},
                        'B83006':{'div':'AWC','practice_code':'SIL'}, 
                        'H81638':{'div':'SUR','practice_code':'SMA'},
                        'M88002':{'div':'BIR','practice_code':'SMC'},
                        'G85005':{'div':'LEW','practice_code':'SLG'},
                        'M91007':{'div':'WAL','practice_code':'SJO'},
                        'Y02626':{'div':'WAL','practice_code':'KEY'},
                        'H81060':{'div':'SUR','practice_code':'TMP'},
                        'K81022':{'div':'WOK','practice_code':'WOK'}}

    ods_code = usage_df['ODS Code'].unique()
    list_size_lookuptable = {}
    for index,row in e_consult_nhse.iterrows():
        for code in ods_code:
            if code == row[0]:
                if code not in list_size_lookuptable:
                    list_size_lookuptable[code] = row[3]

    #dictionary of divisional list lisze from suming practice list sizes
    #obtaining individual divisions
    div = set( val['div'] for val in practice_lookuptable.values())

    #division look up table created
    division_list_size_lut = {}
    for division in div:
        for key in practice_lookuptable:
            if practice_lookuptable[key]['div'] == division:
                if division not in division_list_size_lut:
                    division_list_size_lut[division] = list_size_lookuptable[key]
                else: 
                    division_list_size_lut[division] += list_size_lookuptable[key]
    
    #Data for reaon dataframe
    #To add divison to reson dataframe
    reason_df['DIV'] = reason_df['ODS Code'].apply(lambda x: practice_lookuptable[x]['div'])
    
    #To add practice code to reson dataframe
    reason_df['Code'] = reason_df['ODS Code'].apply(lambda x: practice_lookuptable[x]['practice_code'])

    #To add list size to reason dataframe
    reason_df['List Size'] = reason_df['ODS Code'].apply(lambda x: list_size_lookuptable[x])
    
    #To add divisional list size to reason dataframe
    reason_df['Div List'] = reason_df['ODS Code'].apply(lambda x: division_list_size_lut[practice_lookuptable[x]['div']] )

    #To add age bracket to reason datafram
    reason_df['Age Bracket'] = reason_df['Age'].apply(age_bracket)
    
    #reason per 1000 divisonal size
    reason_df['eConsult reason per 1000'] = 1000 / reason_df['Div List']

    #reson per 1000 practice list size
    reason_df['eConsult per 1000 practice'] = 1000 / reason_df['List Size']

    #Add singular month to df
    reason_df['Month'] = reason_df['Date'].apply(lambda x: x.strftime("%B"))

    #Data for usage df
    #To add list size to usage df
    usage_df['List Size'] = reason_df['ODS Code'].apply(lambda x: list_size_lookuptable[x])

    #eConsults submitted per 1000 practice list size
    usage_df['eConsults submitted per 1000'] =(usage_df['eConsults submitted']/usage_df['List Size'])*1000
    
    #Month is input as the date on the first of the month
    day = "01"
    month = reason_df['Month'][0]
    year = reason_df['Date'][0].strftime("%Y")
    date = day + "/" + month + "/" + year
    usage_df['Month'] = datetime.strptime(date,"%d/%B/%Y")

    #Emiss or S1
    usage_df['EMIS/ S1'] = 'EMIS'

    reason_df = reason_df.drop('ODS Code', axis=1)
    usage_df = usage_df.drop(['ODS Code','Practice Id','Practice Type'], axis=1)
    return usage_df, reason_df
