import localpackage.file_upload_clean as fc
from localpackage.gbq_transfer import df_to_gbq, data_file_info, delete_blob

# def main(data, context):
#     file_path, file_name = data_file_info(data['name'])

#     if file_path == "Budget":
#         budget_df = fc.clean_budget(f"gs://{data['bucket']}/{data['name']}")
#         df_to_gbq(budget_df, "Finance", "Budget")
#         delete_blob(data['bucket'], data['name'])
#     elif file_path == "eConsult":
#         # usage_df, reason_df = fc.clean_consultations(f"gs://{data['bucket']}/{data['name']}")
#         # df_to_gbq(usage_df, "Consultations", "Usage")
#         # df_to_gbq(reason_df, "Consultations", "Reason")
#         # delete_blob(data['bucket'], data['name'])
#         pass

def main():
    test_df_usage, test_df_reason = fc.consultations_clean('eConsult.xlsx')
    df_to_gbq(test_df_usage, "eConsult", "Usage_test")
    #df_to_gbq(test_df_reason, "eConsult", "Reason_test")


if __name__ == "__main__":
    main()