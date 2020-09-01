from localpackage.finance_clean import clean_budget
from localpackage.eConsult_clean import clean_econsult_activity, clean_econsult_survey, clean_econsult_comments
from localpackage.gbq_transfer import df_to_gbq, data_file_info, delete_blob
from phone_data_queue_table import queing_table

def main(data, context):

    file_path, file_name = data_file_info(data['name'])

    # Only applies if uploaded file is either csv or excel
    # if data['contentType'] == "text/plain":
    #     continue

    if file_path == "Budget":
        df_budget = clean_budget(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_budget, "Finance", "Budget")
    elif file_path == "eConsult/Activity":
        df_usage, df_reason = clean_econsult_activity(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_usage, "eConsult", "Usage")
        df_to_gbq(df_reason, "eConsult", "Reason")
    elif file_path == "eConsult/Patient_Survey":
        df_activity = clean_econsult_survey(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_activity, "eConsult", "patient_feedback_response")
    elif file_path == "eConsult/Patient_Comments":
        df_comments = clean_econsult_comments(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_comments, "eConsult", "patient_feedback_comments")

    delete_blob(data['bucket'], data['name'])
