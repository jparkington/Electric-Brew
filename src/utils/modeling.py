from curation         import write_results
from utils.dataframes import *

import logging as lg
import numpy   as np
import pandas  as pd

lg.basicConfig(level  = lg.INFO, 
               format = '%(asctime)s | %(levelname)s | %(message)s')

'''
Contains functions that transform DATAFRAMES into a star schema optimized for analytical queries and 
data visualization. The aim is to create a structured, denormalized data model that enables fast and 
intuitive querying.

Functions:
    - model_dim_datetimes     : Generates a datetime dimension table from `meter_usage` timestamps.
    - model_dim_meters        : Extracts account numbers, service points, streets, and labels.
    - model_dim_bills         : Groups by common dimensions and aggregates relevant metrics across all billing sources.
    - model_fct_electric_brew : Generates a central fact table of all electric usage records and their associated charges.
'''

def model_dim_datetimes(model: str = "./data/modeled/dim_datetimes"):
    '''
    This function creates a datetime dimension table from the `meter_usage` DataFrame.
    It extracts unique timestamps, generates various time components, and saves the result as a .parquet file.
    
    Methodology:
        1. Extract unique timestamps from `meter_usage['interval_end_datetime']` and sort them.
        2. Create a DataFrame with these timestamps and generate time components such as increment, hour, etc.
        3. Define the period of the day based on the hour.
        4. Save the DataFrame as a .parquet file in the specified `model` directory with snappy compression.
        
    Parameters:
        model (str): Directory where the .parquet file should be saved.
    '''
    
    try:
        # Step 1: Extract unique timestamps and sort them
        timestamps = pd.to_datetime(meter_usage['interval_end_datetime'].unique(), format = '%m/%d/%Y %I:%M:%S %p')
        timestamps = np.sort(timestamps)

        # Step 2: Create a DataFrame for the datetime dimension
        df = pd.DataFrame(timestamps, columns = ['timestamp'])
        
        # Generate standard datetime components from the timestamp
        df['increment']    = df['timestamp'].dt.minute
        df['hour']         = df['timestamp'].dt.hour
        df['date']         = df['timestamp'].dt.normalize()
        df['week']         = df['timestamp'].dt.isocalendar().week
        df['week_start']   = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)
        df['month']        = df['timestamp'].dt.month
        df['month_name']   = df['timestamp'].dt.month_name()
        df['month_start']  = df['timestamp'].dt.to_period('M').apply(lambda r: r.start_time)
        df['quarter']      = df['timestamp'].dt.quarter
        df['year']         = df['timestamp'].dt.year

        # Step 3: Define the period based on the hour
        df['period'] = df['hour'].apply(
            lambda hour: 'Off-peak: 12AM to 7AM' if 0 <= hour < 7 else (
                         'Mid-peak: 7AM to 5PM, 9PM to 11PM' if (7 <= hour < 17) or (21 <= hour < 23) else 
                         'On-peak: 5PM to 9PM'))
        
        # Step 4: Save the DataFrame as a .parquet file
        write_results(data         = df, 
                      dest         = model,
                      add_id       = True,
                      partition_by = None)

    except Exception as e:
        lg.error(f"Error creating datetime dimension table: {e}\n")

def model_dim_meters(model: str = "./data/modeled/dim_meters"):
    '''
    This function creates a meters dimension table by joining data from the `meter_usage` and `locations` DataFrames.
    It extracts account numbers, service points, meter IDs, streets, and labels, and saves the result as a .parquet file.

    Methodology:
        1. Extract and join relevant columns based on `account_number`.
        2. Save the resulting DataFrame as a .parquet file in the specified `model` directory with snappy compression.

    Parameters:
        model (str): Directory where the .parquet file should be saved.
    '''

    try:
        # Step 1: Extract and join relevant columns
        df = pd.merge(meter_usage[['meter_id', 'service_point_id', 'account_number']].drop_duplicates(), 
                      locations[['account_number', 'street', 'label']].drop_duplicates(), 
                      on  = 'account_number', 
                      how = 'left')

        # Step 2: Save the DataFrame as a .parquet file
        write_results(data         = df, 
                      dest         = model,
                      add_id       = True,
                      partition_by = None)

    except Exception as e:
        lg.error(f"Error creating meters dimension table: {e}\n")

def model_dim_bills(model: str = "./data/modeled/dim_bills"):
    '''
    This function creates a bills dimension table from both the `cmp_bills` and `ampion_bills` DataFrames.
    It groups by common dimensions, aggregates relevant metrics, and concatenates the results from both DataFrames.

    Methodology:
        1. Group `cmp_bills` and `ampion_bills` by common dimensions and aggregate metrics.
        2. Concatenate the results and assign a source identifier for each row.
        3. Replace `interval_start` and `interval_end` with `billing_interval`
        4. Save the resulting DataFrame as a .parquet file in the specified `model` directory with snappy compression.
    
    Parameters:
        model (str): Directory where the .parquet file should be saved.
    '''

    try:
        # Step 1: Define common dimensions, standardize against them, and aggregate numerics
        common_dims = ['invoice_number', 'account_number', 'interval_start', 'interval_end', 'supplier']

        # Standardize `cmp_bills`
        df1 = cmp_bills.apply(lambda col: col.fillna(0) if col.dtype.kind in 'biufc' else col)
        df1['kwh_delivered']
        df1['service_charge']
        df1['taxes']         = df1['delivery_tax']    + df1['supply_tax']
        df1['delivery_rate'] = df1['delivery_charge'] / df1['kwh_delivered']
        df1['supply_rate']   = df1['supply_charge']   / df1['kwh_supplied']
        df1['source']        = "CMP"

        # Standardize `ampion_bills`
        df2 = ampion_bills.groupby(common_dims, observed = True) \
                          .agg(kwh_delivered  = ('kwh',   'sum'), 
                               price          = ('price', 'sum')) \
                          .reset_index()
        df2['service_charge'] = 0
        df2['taxes']          = 0
        df2['delivery_rate']  = 0
        df2['supply_rate']    = df2['price'] / df2['kwh_delivered']
        df2['source']         = "Ampion"
        df2.drop(columns = ['price'], inplace = True)

        # Step 2: Concatenate standardize dataframes
        df1 = df1[df2.columns]
        df = pd.concat([df1, df2], ignore_index = True)

        # Step 3: Replace `interval_start` and `interval_end` with `billing_interval`
        df['billing_interval'] = [pd.date_range(s, e, inclusive = 'both').date.tolist() 
                                  for s, e in zip(df['interval_start'], df['interval_end'])]
        df.drop(columns = ['interval_start', 'interval_end'], inplace = True)

        # Step 4: Save the DataFrame as a .parquet file
        write_results(data         = df, 
                      dest         = model,
                      add_id       = True,
                      partition_by = None)

    except Exception as e:
        lg.error(f"Error creating bills dimension table: {e}\n")

def model_fct_electric_brew(model: str  = "./data/modeled/fct_electric_brew"):
    
    '''
    This function generates a central fact table recording electric usage and associated charges for each account per time interval.
    It integrates data from meter readings, customer billing, and rate information, applying business rules to calculate the cost
    of electric delivery and usage.

    Methodology:
        1. Expand 'dim_bills' to create daily granularity based on the billing interval and group by source.
        2. Merge expanded billing data with meter usage and dimension tables.
        3. Calculate total kWh recorded for each invoice number and kWh delivered.
        4. Compute remaining kWh and used kWh per reading for CMP billing, starting at the end of each interval.
        5. Compute remaining kWh and used kWh per reading for Ampion billing, starting at the beginning of each interval.
        6. Calculate delivery and supply costs based on used kWh, and allocate service and tax costs.
        7. Save the resulting DataFrame as a .parquet file in the specified 'model' directory with snappy compression.

    Parameters:
        model (str): Directory where the .parquet file should be saved.
    '''
    
    try:
        # Step 1: Expand 'dim_bills' and group by source
        explode = {s: df for s, df in dim_bills.explode('billing_interval')
                                               .assign(date = lambda x: pd.to_datetime(x['billing_interval']))
                                               .groupby('source')}

        # Step 2: Merge with meter usage and dimension tables
        bill_fields = ['account_number', 'date']
        int_df = meter_usage.assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], format = '%m/%d/%Y %I:%M:%S %p')) \
                            .merge(dim_datetimes,     on = 'timestamp', how = 'left', suffixes = ('', '_dat')) \
                            .merge(dim_meters,        on = 'meter_id',  how = 'left', suffixes = ('', '_met')) \
                            .merge(explode['CMP'],    on = bill_fields, how = 'left', suffixes = ('', '_cmp')) \
                            .merge(explode['Ampion'], on = bill_fields, how = 'left', suffixes = ('', '_amp')) \
                            .apply(lambda col: col.fillna(0) if col.dtype.kind in 'biufc' else col)

        # Step 3: Calculate total kWh recorded
        int_df['total_recorded_kwh'] = int_df.groupby(['invoice_number', 'kwh_delivered'])['kwh'].transform('sum')
        int_df['kwh_ratio']          = int_df['kwh'] / int_df['total_recorded_kwh']

        # Steps 4 & 5: Calculate remaining and used kWh for CMP and Ampion
        cmp_waterfall = int_df.sort_values(by = ['invoice_number', 'timestamp'])
        int_df['delivered_kwh_left'] = cmp_waterfall.groupby(['invoice_number', 'kwh_delivered'], group_keys = False) \
                                                    .apply(lambda g: (g['kwh_delivered'].iloc[0] - g['kwh'].iloc[::-1].cumsum()).clip(lower = 0))
        int_df['delivered_kwh_used'] = np.minimum(int_df['kwh'], int_df['delivered_kwh_left']).clip(lower = 0)

        ampion_waterfall = int_df.sort_values(by = ['invoice_number_amp', 'timestamp'])
        int_df['ampion_kwh_left'] = ampion_waterfall.groupby(['invoice_number_amp', 'kwh_delivered_amp'], group_keys = False) \
                                                    .apply(lambda g: (g['kwh_delivered_amp'].iloc[0] - g['kwh'].cumsum()).clip(lower = 0))
        int_df['ampion_kwh_used'] = np.minimum(int_df['kwh'], int_df['ampion_kwh_left']).clip(lower = 0)
        int_df = int_df.apply(lambda col: col.fillna(0) if col.dtype.kind in 'biufc' else col)

        # Step 6: Compute cost metrics
        df = pd.DataFrame(index = int_df.index)
        df['dim_datetimes_id']  = int_df['id']
        df['dim_meters_id']     = int_df['id_met']
        df['dim_bills_id']      = np.where(int_df['ampion_kwh_used'] > 0, int_df['id_amp'], int_df['id_cmp'])
        df['account_number']    = int_df['account_number']
        df['kwh']               = int_df['kwh']
        df['delivery_cost']     = int_df['delivered_kwh_used'] * int_df['delivery_rate']
        df['service_cost']      = int_df['service_charge']     * int_df['kwh_ratio']
        df['supply_cost']       = int_df['delivered_kwh_used'] * int_df['supply_rate'] + int_df['ampion_kwh_used'] * int_df['supply_rate_amp']
        df['tax_cost']          = int_df['taxes']              * int_df['kwh_ratio']
        df['total_cost']        = df['delivery_cost'] + df['service_cost'] + df['supply_cost'] + df['tax_cost']

        # Step 7: Save the DataFrame as a .parquet file
        write_results(data   = df, 
                      dest   = model,
                      add_id = True)

    except Exception as e:
        lg.error(f"Error while creating the final fact table: {e}\n")