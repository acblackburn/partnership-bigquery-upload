import file_upload_clean as fc
from gbq_transfer import df_to_gbq, event_file_info

def main(event, context):
    file_path, file_name = event_file_info(event['name'])
    
    if file_path == "Budget":
        budget_df = fc.clean_budget(event)
        df_to_gbq(budget_df, "Finance", "Budget")
    elif file_path == "eConsult":
        pass

    # Delete original uploaded file

""" def main():
    budget_df = fc.clean_budget("Budgetv1.csv")
    print(budget_df.head())
    df_to_gbq(budget_df, "Finance", "Budget")

if __name__ == "__main__":
    main() """