import pandas as pd
import numpy as np
import json
from datetime import datetime
import re

def group_split(question):
    """Assigns a patient survey question to a group"""
    if question in ["Q3", "Q4", "Q9", "Q25", "Q28"]:
        return 'Your local GP services'
    elif question in ["Q18", "Q79", "Q80"]:
        return "Making an appointment"
    elif question in ["Q85", "Q86a", "Q86b", "Q86e", "Q87", "Q88", "Q89", "Q90"]:
        return "Your last appointment"
    elif question=="Q32":
        return "Your health"
    else:
        return "-"

def patient_survey_clean(input_file):
    year = re.findall(r'\d+', input_file)[0]

    # Open data
    df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_data.csv")
    map_df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_reporting_var.csv", encoding = 'cp1252')

    #encoding = 'cp1252'

    map_df.rename(columns = {'ï»¿Variable':'Variable'}, inplace=True)

    # Open JSON file
    with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)
    # with open("question_key.json") as json_file:
    #     question_key = json.load(json_file)

    # Filter only modality practices from ODS codes in JSON file
    ods_code = [entry["ODS Code"] for entry in practice_lookup]
    df_modality = df[df['Practice_Code'].isin(ods_code)]

    df_modality.columns = df_modality.columns.str.upper()

    # Isolating unique questions
    questions = df_modality.columns
    questions_q = questions[questions.str.startswith('Q')]
    questions_orig = questions_q[~questions_q.str.contains('BASE')]
    questions_orig = questions_orig.str.upper().str.split('_').str[0].unique()

    # New list
    new_df_list = []

    #find column index off CCG name and code
    index_CCG_name = df_modality.columns.get_loc("CCG_NAME")
    index_CCG_code = df_modality.columns.get_loc("CCG_CODE")

    # Loop through each queestion
    for question_no in questions_orig:
        get_loc_list = df_modality.columns

        # Split to get just q number
        get_loc_list = get_loc_list.str.split('_').str[0]
        index_list = [get_loc_list.get_loc(question_no)]

        index_list = np.where(index_list)[1]
        index_list = index_list.tolist()

        # Looping through each practice
        for i in range(0, len(df_modality)):
            row = df_modality.iloc[i]

            # Splits it into my table format
            for index in index_list:
                combined_question = df_modality.columns[index]
                response = df_modality.columns[index].split('_')[1]
                question = question_no
                if row[index] < 0:
                    value = 0
                else:
                    value = row[index]*100
                row_add = [row[0], row[index_CCG_code], row[index_CCG_name], question, response, combined_question, value]
                new_df_list.append(row_add)

    new_df = pd.DataFrame(new_df_list, columns=["ODS_Code","CCG_Code","CCG_Name", "Question", "Response","Combined_Question","Value"])

    new_df = new_df[new_df['Response'].str.contains('PCT')]

    # Add division column to reason dataframe
    new_df['DIV'] = new_df['ODS_Code'].map({entry['ODS Code']:entry['DIV'] for entry in practice_lookup})

    # Add practice code column to reason dataframe
    new_df['Practice_Code'] = new_df['ODS_Code'].map({entry['ODS Code']:entry['practice_code'] for entry in practice_lookup})

    # Add year in datetime format, 01/01/2019
    day = "01"
    month = "01"
    date = day + "/" + month + "/" + str(year)
    new_df['Year'] = datetime.strptime(date,"%d/%m/%Y")

    #Open question to description mapping file
    map_df['Variable'] = map_df['Variable'].str.upper()
    map_df = map_df[map_df['Variable'].str.startswith('Q')]
    question_map = pd.Series(map_df['Description'].values, index=map_df['Variable']).to_dict()
    response_order_map = pd.Series(map_df['Response_Order'].values, index=map_df['Variable']).to_dict()

    new_df['Response_Order'] = new_df['Combined_Question'].map(response_order_map)
    new_df['Combined_Response'] = new_df['Combined_Question'].map(question_map)
    new_df['Question_String'] = new_df['Combined_Response'].str.split(' - ').str[0].str.strip()
    new_df['Response_String'] = new_df['Combined_Response'].str.split(' - ').str[1].str.strip()

    new_df['Grouped'] = new_df['Question'].apply(group_split)

    new_df['CCG_Name'] = new_df['CCG_Name'].str.upper()
    print(new_df['CCG_Name'].unique())
    return new_df

def survey_response_rate(input_file):
    year = re.findall(r'\d+', input_file)[0]
    # Open data
    df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_data.csv", encoding = 'cp1252')

    # Open JSON file
    with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)

    # Filter only modality practices from ODS codes in JSON file
    ods_code = [entry["ODS Code"] for entry in practice_lookup]
    df_modality = df[df['Practice_Code'].isin(ods_code)]

    new_df = df_modality[['Practice_Code',"resprate","distributed","received"]]

    new_df.rename(columns={"Practice_Code":"ODS_Code"}, inplace=True)

    #percentage of reponse rate
    new_df['perc_resprate'] = new_df['resprate']*100

    # Add division column to reason dataframe
    new_df['DIV'] = new_df['ODS_Code'].map({entry['ODS Code']:entry['DIV'] for entry in practice_lookup})
    # Add practice code column to reason dataframe
    new_df['Practice_Code'] = new_df['ODS_Code'].map({entry['ODS Code']:entry['practice_code'] for entry in practice_lookup})

    # Add year in datetime format, 01/01/2019
    day = "01"
    month = "01"
    date = day + "/" + month + "/" + str(year)
    new_df['Year'] = datetime.strptime(date,"%d/%m/%Y")

    return new_df

def ccg_survey_clean(file_name):
    year = re.findall(r'\d+', file_name)[0]
    print(year)
    #list of CCGs
    CCG = [
            'NHS HULL CCG', 'NHS BRADFORD DISTRICT AND CRAVEN CCG',
            'NHS SOUTH EAST LONDON CCG', 'NHS SURREY HEARTLANDS CCG',
            'NHS WEST SUSSEX CCG', 'NHS BERKSHIRE WEST CCG',
            'NHS SANDWELL AND WEST BIRMINGHAM CCG', 'NHS BIRMINGHAM AND SOLIHULL CCG',
            'NHS WALSALL CCG', 'NHS AIREDALE', 'WHARFEDALE AND CRAVEN CCG',
            'NHS LEWISHAM CCG', 'NHS EAST SURREY CCG', 'NHS HORSHAM AND MID SUSSEX CCG',
            'NHS BIRMINGHAM SOUTH AND CENTRAL CCG', 'NHS WOKINGHAM CCG', 'NHS BIRMINGHAM CROSSCITY CCG'
            ]
    # Open data
    df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_CCG.csv")
    map_df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_reporting_var.csv",encoding = 'cp1252')

    #encoding = 'cp1252'

    map_df.rename(columns = {'ï»¿Variable':'Variable'}, inplace=True)

    # Open JSON file
    with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)

    #Calitalising column titles
    df.columns = df.columns.str.upper()

    #Checking that all CCGs are capitalised
    df['CCG_NAME'] = df['CCG_NAME'].str.upper()
    df_ccg = df[df['CCG_NAME'].isin(CCG)]

     #find column index off CCG name and code
    index_CCG_name = df_ccg.columns.get_loc("CCG_NAME")
    index_CCG_code = df_ccg.columns.get_loc("CCG_CODE")

    # Isolating unique questions
    questions = df_ccg.columns
    questions_q = questions[questions.str.startswith('Q')]
    questions_orig = questions_q[~questions_q.str.contains('BASE')]
    questions_orig = questions_orig.str.upper().str.split('_').str[0].unique()

    # New list
    new_df_list = []

    # Loop through each queestion
    for question_no in questions_orig:
        get_loc_list = df_ccg.columns

        # Split to get just q number
        get_loc_list = get_loc_list.str.split('_').str[0]
        index_list = [get_loc_list.get_loc(question_no)]


        index_list = np.where(index_list)[1]
        index_list = index_list.tolist()

        # Looping through each practice
        for i in range(0, len(df_ccg)):
            row = df_ccg.iloc[i]

            # Splits it into my table format
            for index in index_list:
                combined_question = df_ccg.columns[index]
                response = df_ccg.columns[index].split('_')[1]
                question = question_no
                if row[index] < 0:
                    value = 0
                else:
                    value = row[index]*100
                row_add = [row[index_CCG_code], row[index_CCG_name], question, response, combined_question, value]
                new_df_list.append(row_add)

    new_df = pd.DataFrame(new_df_list, columns=["CCG_Code","CCG_Name", "Question", "Response","Combined_Question","Value"])
    new_df = new_df[new_df['Response'].str.contains('PCT')]

   # Add year in datetime format, 01/01/2019
    day = "01"
    month = "01"
    date = day + "/" + month + "/" + str(year)
    new_df['Year'] = datetime.strptime(date,"%d/%m/%Y")

    #Open question to description mapping file
    map_df['Variable'] = map_df['Variable'].str.upper()
    map_df = map_df[map_df['Variable'].str.startswith('Q')]
    question_map = pd.Series(map_df['Description'].values, index=map_df['Variable']).to_dict()
    response_order_map = pd.Series(map_df['Response_Order'].values, index=map_df['Variable']).to_dict()

    new_df['Combined_Response'] = new_df['Combined_Question'].map(question_map)
    new_df['Question_String'] = new_df['Combined_Response'].str.split(' - ').str[0].str.strip()
    new_df['Response_String'] = new_df['Combined_Response'].str.split(' - ').str[1].str.strip()
    new_df['Response_Order'] = new_df['Combined_Question'].map(response_order_map)

    new_df['Grouped'] = new_df['Question'].apply(group_split)
    return new_df

def nat_survey_clean(input_file):
    year = re.findall(r'\d+', input_file)[0]

    # Open data
    df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_Nat.csv")
    map_df = pd.read_csv(f"gs://patient_survey_data_upload/{year}_reporting_var.csv",encoding = 'cp1252')
    #encoding = 'cp1252'

    map_df.rename(columns = {'ï»¿Variable':'Variable'}, inplace=True)

    # Open JSON file
    with open("practice_lookup.json") as json_file:
        practice_lookup = json.load(json_file)

    #Calitalising column titles
    df.columns = df.columns.str.upper()

    # Isolating unique questions
    questions = df.columns
    questions_q = questions[questions.str.startswith('Q')]
    questions_orig = questions_q[~questions_q.str.contains('BASE')]
    questions_orig = questions_orig.str.upper().str.split('_').str[0].unique()
    # New list
    new_df_list = []

    # Loop through each queestion
    for question_no in questions_orig:
        get_loc_list = df.columns[df.columns.str.startswith(question_no)]
        get_loc_list = get_loc_list[get_loc_list.str.contains("PCT")]
        # Splits it into my table format

        if len(get_loc_list) > 0:
            for title in get_loc_list:
                combined_question = title
                response = title.split('_')[1]
                question = question_no
                if df[title][0] < 0:
                    value = 0
                else:
                    value = df[title][0]*100
                row_add = [question, response, combined_question, value]
                new_df_list.append(row_add)

    new_df = pd.DataFrame(new_df_list, columns=["Question", "Response","Combined_Question","Value"])
    new_df = new_df[new_df['Response'].str.contains('PCT')]

   # Add year in datetime format, 01/01/2019
    day = "01"
    month = "01"
    date = day + "/" + month + "/" + str(year)
    new_df['Year'] = datetime.strptime(date,"%d/%m/%Y")

    #Open question to description mapping file
    map_df['Variable'] = map_df['Variable'].str.upper()
    map_df = map_df[map_df['Variable'].str.startswith('Q')]
    question_map = pd.Series(map_df['Description'].values, index=map_df['Variable']).to_dict()
    response_order_map = pd.Series(map_df['Response_Order'].values, index=map_df['Variable']).to_dict()

    new_df['Combined_Response'] = new_df['Combined_Question'].map(question_map)
    new_df['Question_String'] = new_df['Combined_Response'].str.split(' - ').str[0].str.strip()
    new_df['Response_String'] = new_df['Combined_Response'].str.split(' - ').str[1].str.strip()
    new_df['Response_Order'] = new_df['Combined_Question'].map(response_order_map)

    new_df['Grouped'] = new_df['Question'].apply(group_split)

    print(new_df['Grouped'].unique())
    return new_df
