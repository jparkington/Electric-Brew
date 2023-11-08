from utils import *

meter_usage
locations
cmp_bills

def create_dim_datetimes(model = "./data/model/dim_datetimes",):

    # Extract unique timestamps and sort them
    timestamps = pd.to_datetime(meter_usage['interval_end_datetime'].unique())
    timestamps.sort()

    # Create a DataFrame for the datetime dimension
    df = pd.DataFrame(timestamps, columns = ['timestamp'])
    df.index = np.arange(1, len(df) + 1)

    # Break down timestamp into standard datetime components
    df['increment']    = df['timestamp'].dt.minute
    df['hour']         = df['timestamp'].dt.hour
    df['date']         = df['timestamp'].dt.date
    df['week']         = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time.date())
    df['week_in_year'] = df['timestamp'].dt.isocalendar().week
    df['month']        = df['timestamp'].dt.month
    df['month_name']   = df['timestamp'].dt.month_name()
    df['quarter']      = df['timestamp'].dt.quarter
    df['year']         = df['timestamp'].dt.year

    # Define the period based on the hour
    df['period'] = df['hour'].apply(
        lambda hour: 'Off-peak: 12AM to 7AM' if 0 <= hour < 7 else (
                     'Mid-peak: 7AM to 5PM, 9PM to 11PM' if (7 <= hour < 17) or (21 <= hour < 23) else 
                     'On-peak: 5PM to 9PM'))

    pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index = True), 
                        root_path      = model, 
                        compression    = 'snappy', 
                        use_dictionary = True)

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

print(meter_usage)