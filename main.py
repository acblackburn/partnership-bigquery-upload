import localpackage.file_upload_clean as fc
from localpackage.gbq_transfer import df_to_gbq, event_file_info, delete_blob

# def main(data, context):
#     file_path, file_name = event_file_info(data['name'])

#     if file_path == "Budget":
#         budget_df = fc.clean_budget(f"gs://{data['bucket']}/{data['name']}")
#         df_to_gbq(budget_df, "Finance", "Budget_test")
#         delete_blob(data['bucket'], data['name'])
#     elif file_path == "eConsult":
#         pass

def main():
    budget_df = fc.clean_budget("Budgetv1.csv")
    # df_to_gbq(budget_df, "Finance", "Budget_test")
    print(budget_df)

if __name__ == "__main__":
    main()