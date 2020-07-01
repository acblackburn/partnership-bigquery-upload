import localpackage.file_upload_clean as fc
from localpackage.gbq_transfer import df_to_gbq, data_file_info, delete_blob

def main(data, context):
    file_path, file_name = data_file_info(data['name'])

    if file_path == "Budget":
        budget_df = fc.clean_budget(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(budget_df, "Finance", "Budget")
    elif file_path == "eConsult":
        test_df_usage, test_df_reason = fc.consultations_clean(f"gs://{data['bucket']}/{data['name']}")
        df_to_gbq(test_df_usage, "eConsult", "Usage")
        df_to_gbq(test_df_reason, "eConsult", "Reason")
    
    delete_blob(data['bucket'], data['name'])