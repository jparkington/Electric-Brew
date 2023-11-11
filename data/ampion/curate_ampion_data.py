''' 
    This file use the functions in 'ampion_util_functions' to read in and process the 
    Ampion bill data. It will generate csv and parquet files for each of the 7 accounts 
    assiciated with Austin Street.
'''
from ampion_utility_functions import get_data
from ampion_utility_functions import export_csv_and_parquet_files

def main():
    
    df, account_numbers = get_data()
    export_csv_and_parquet_files(df, account_numbers)

if __name__ == "__main__":
    main()