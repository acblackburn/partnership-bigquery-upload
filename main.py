import localpackage.file_upload_clean as fc
from localpackage.gbq_transfer import df_to_gbq, data_file_info, delete_blob

# def main(data, context):
#     file_path, file_name = data_file_info(data['name'])

#     if file_path == "Budget":
#         budget_df = fc.clean_budget(f"gs://{data['bucket']}/{data['name']}")
#         df_to_gbq(budget_df, "Finance", "Budget")
#     elif file_path == "eConsult":
#         usage_df, reason_df = fc.consultations_clean(f"gs://{data['bucket']}/{data['name']}")
#         df_to_gbq(usage_df, "eConsult", "Usage")
#         df_to_gbq(reason_df, "eConsult", "Reason")
    
#     delete_blob(data['bucket'], data['name'])

def main():
    test_df_usage, test_df_reason = fc.consultations_clean("gs://dashboards_data_upload/eConsult/eConsult_usage_report_for_Modality__2020060120200630.xlsx")
    # df_to_gbq(test_df_reason, "eConsult", "Reason")
    df_to_gbq(test_df_usage, "eConsult", "Usage")

if __name__ == "__main__":
    main()