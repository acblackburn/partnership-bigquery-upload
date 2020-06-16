import clean_functions as cf
from gbq_transfer import df_to_gbq, event_file_info

def main(event, context):
    file_path, file_name = event_file_info(event['name'])
    
    # Clean group metrics data
    gm_df = cf.clean_groupmetrics("group_metrics.xlsx")

    # load cleaned data to BigQuery
    df_to_gbq(gm_df, "group_metrics_test")

if __name__ == "__main__":
    main()