''' 
    This file use the functions in 'ampion_util_functions' to read in and process the 
    Ampion bill data. It will generate csv and parquet files for each of the 7 accounts 
    assiciated with Austin Street.
'''
from ampion_utility_functions import get_data
from ampion_utility_functions import export_csv_and_parquet_files
from ampion_utility_functions import curate_meter_usage

def main():
    
    df, account_numbers = get_data()
    export_csv_and_parquet_files(df, account_numbers)
    curate_meter_usage('data/ampion/curated/csv_files','data/ampion/curated/parquet_files', ['abbr_account_number'],
                        ['abbr_account_number',' invoice_number' , 'start_date' , 'end_date', 'production?' , 'bill_credits_allocated', 'bill_credit_received', 'price']  )


if __name__ == "__main__":
    main()