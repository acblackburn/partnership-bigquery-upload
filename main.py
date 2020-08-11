import localpackage.file_upload_clean as fc
from localpackage.gbq_transfer import df_to_gbq, data_file_info, delete_blob
from phone_data_queue_table import queing_table

def main(data, context):
    file_path, file_name = data_file_info(data['name'])
    
    if file_path == "Budget":
        df_budget = fc.clean_budget(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_budget, "Finance", "Budget")
    elif file_path == "eConsult/Activity":
        df_usage, df_reason = fc.clean_econsult_activity(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_usage, "eConsult", "Usage")
        df_to_gbq(df_reason, "eConsult", "Reason")
    elif file_path == "eConsult/Patient_Survey":
        df_activity = fc.clean_econsult_activity(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_activity, "eConsult", "patient_feedback_response")
    elif file_path == "eConsult/Patient_Comments":
        df_comments = fc.clean_econsult_comments(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(df_comments, "eConsult", "patient_feedback_comments")
    
    delete_blob(data['bucket'], data['name'])

def main():
    data = "phone_data_clean.csv"
    df_queue = queing_table(data)
    df_to_gbq(df_queue, "phone_data", "queing_table")

if __name__ == "__main__":
    main()