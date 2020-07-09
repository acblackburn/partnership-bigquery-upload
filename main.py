import localpackage.file_upload_clean as fc
from localpackage.gbq_transfer import df_to_gbq, data_file_info, delete_blob

# def main(data, context):
#     file_path, file_name = data_file_info(data['name'])
    
#     if file_path == "Budget":
#         df_budget = fc.clean_budget(f"gs://{data['bucket']}/{data['name']}")
#         df_to_gbq(df_budget, "Finance", "Budget")
#     if file_path == "eConsult":
#         df_usage = fc.clean_consultations(f"gs://{data['bucket']}/{data['name']}")
#         df_to_gbq(df_usage, "eConsult", "Usage")
#         # df_to_gbq(df_reason, "eConsult", "Reason")

#     delete_blob(data['bucket'], data['name'])

def main():
    data = "gs://dashboards-data-upload/eConsult/eConsult_usage_report_for_Modality__2020060120200630.xlsx"
    df_usage, df_reason = fc.clean_consultations(data)
    df_to_gbq(df_usage, "eConsult", "Usage")
    df_to_gbq(df_reason, "eConsult", "Reason")

if __name__ == "__main__":
    main()