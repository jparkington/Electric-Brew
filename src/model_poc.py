from utils import *

def create_int_all_sources(curated       : str  = "./data/model/int_all_sources",
                           partition_col : list = ['account_number']):
    '''
    This function takes the `meter_usage`, `locations`, and `cmp_bills` DataFrames, performs left joins on 
    `account_number`, and then saves the combined data as a partitioned .parquet file in the `model` directory with
    snappy compression.
    
    Methodology:
        1. Perform a left join between `meter_usage` and `locations` on `account_number`.
        2. Save the resulting DataFrame as a .parquet file in the `model` directory, partitioned by `partition_col`.
        
    Parameters:
        curated       (str)          : Directory where the partitioned .parquet files should be saved.
        partition_col (list)         : Column name(s) to use for partitioning the parquet files.
    '''
    
    try:
        # Step 1: Perform a left join between `meter_usage` and `locations` on `account_number`
        int_df = pd.merge(meter_usage, locations, on = 'account_number', how = 'left')
        
        # # Step 2: Save the resulting DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`
        # if not os.path.exists(curated):
        #     lg.warning(f"Directory {curated} does not exist. It will now be created.")
        #     os.makedirs(curated)

        # pq.write_to_dataset(pa.Table.from_pandas(merged_df, 
        #                                          preserve_index = False), 
        #                     root_path      = curated, 
        #                     partition_cols = partition_col, 
        #                     compression    = 'snappy', 
        #                     use_dictionary = True)
        
        # lg.info(f"Data saved as partitioned .parquet files in {curated}.")

    except Exception as e:
        lg.error(f"Error while joining meter and locations data: {e}")

    return int_df

print(dim_suppliers)