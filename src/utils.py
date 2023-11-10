from cycler            import cycler
from glob              import glob
from matplotlib.pyplot import rcParams
from re                import search, DOTALL
from seaborn           import color_palette
from sqlalchemy        import *
from sqlalchemy.exc    import SQLAlchemyError

import os
import logging         as lg
import numpy           as np
import pandas          as pd
import pdfplumber      as pl
import pyarrow         as pa
import pyarrow.parquet as pq

# Set up a logging configuration
lg.basicConfig(level  = lg.INFO, 
               format = '%(asctime)s | %(levelname)s | %(message)s')

'''
=========================================
================ RUNTIME ================
=========================================

Contains utility functions that configure the runtime environment and are called as scripts are
executed. These include `rcParams` and specific paradigms for reading data into dataframes.

Functions:
    - set_plot_params : Sets up custom plot parameters for matplotlib.
    - read_data       : Reads a .parquet file into a Pandas DataFrame.
'''

def set_plot_params() -> list:
    '''
    This function sets up custom plot parameters for matplotlib plots.
    
    The function defines various parameters related to axes, figure, font, text, grid, and legend of the plot.
    These parameters are then used to update matplotlib's runtime configuration, ensuring all subsequent plots
    adhere to this custom style.

    Returns:
        colors (list) : A list of RGBA color tuples that make up the custom color palette.
    '''

    colors = color_palette("Set2")

    rcParams.update({# Axes parameters                           # Tick parameters
                    'axes.facecolor'     : '.1',                 'xtick.labelsize'    : 8,
                    'axes.grid'          : True,                 'xtick.color'        : 'white',
                    'axes.labelcolor'    : 'white',              'xtick.major.size'   : 0,
                    'axes.spines.left'   : False,                'ytick.labelsize'    : 8,
                    'axes.spines.right'  : False,                'ytick.color'        : 'white',
                    'axes.spines.top'    : False,                'ytick.major.size'   : 0,
                    'axes.labelsize'     : 8,
                    'axes.labelweight'   : 'bold',               # Figure parameters
                    'axes.titlesize'     : 12,                   'figure.facecolor'   : 'black',
                    'axes.titleweight'   : 'bold',               'figure.figsize'     : (15, 10),
                    'axes.labelpad'      : 15,                   'figure.autolayout'  : True,
                    'axes.titlepad'      : 15,

                    # Font and text parameters                   # Legend parameters
                    'font.family'        : 'DejaVu Sans Mono',   'legend.facecolor'   : '0.3',
                    'font.size'          : 8,                    'legend.edgecolor'   : '0.3',
                    'font.style'         : 'normal',             'legend.borderpad'   : 0.75,
                    'text.color'         : 'white',              'legend.framealpha'  : '0.5',

                    # Grid parameters
                    'grid.linestyle'     : ':',
                    'grid.color'         : '0.3',
                    'axes.prop_cycle'    : cycler(color = colors)})
    
    return colors

def read_data(file_path: str) -> pd.DataFrame:
    '''
    This function reads a .parquet file from a specified relative path into a Pandas DataFrame.
    The function automatically resolves the path relative to the project's /data/ directory.
    
    Steps:
    1. Get the absolute path of the current file (utils.py) using `os.path.abspath(__file__)`.
    2. Determine the directory of the current file (`src`) using `os.path.dirname()`.
    3. Navigate to the project root directory by going up one level with `..`.
    4. Append 'data' to the project root directory to reach the /data/ directory.
    5. Join this with the user-provided `file_path` to construct the full path to the .parquet file.
    6. Conditionally set the first column to an index if it's named `id`
    
    Parameters:
        file_path (str) : Relative path to the .parquet file, starting from the /data/ directory.
        
    Returns:
        pd.DataFrame : DataFrame containing the data read from the .parquet file.
    '''
    
    current_file_path = os.path.abspath(__file__)           # Get the absolute path of the current file (utils.py)
    src_directory     = os.path.dirname(current_file_path)  # Get the directory of the file at runtime
    project_root      = os.path.join(src_directory, '..')   # Go up one directory to get to the project root
    data_directory    = os.path.join(project_root, 'data')  # Join with 'data' to get to the data directory
    full_file_path    = os.path.abspath(os.path.join(data_directory, file_path))

    df = pq.read_table(full_file_path).to_pandas()

    # Check if the first column is 'id' and set it as the index
    if df.columns[0] == 'id':
        df.set_index('id', inplace = True)

    return df

'''
=========================================
============== DATAFRAMES ===============
=========================================

Contains commonly used DataFrames initialized at the start for easier access across different scripts. 
These DataFrames are curated and optimized for efficient data operations.

DataFrames:
    - meter_usage       : Contains meter usage data from CMP.
    - cmp_bills         : Contains billed delivery and supplier rates for various periods of activity.
    - locations         : Adds manual CSV entries describing each of the accounts Austin St. uses to the model.
    - dim_datetimes     : Breaks down timestamps into individual date and time components, with categorization of periods.
    - dim_meters        : Abstracts the account numbers, service points, meter IDs, and streets dimensions into one table.
    - dim_suppliers     : Extracts the supplier name and calculates the average supply rate as a reference dimension.
    - fct_electric_brew : Houses all the model's facts about usage, billing, and the cost of delivery.
'''

# Curated sources
meter_usage = read_data("cmp/curated/meter-usage")
cmp_bills   = read_data("cmp/curated/bills")
locations   = read_data("cmp/curated/locations")

# Model
dim_datetimes     = read_data("model/dim_datetimes")
dim_meters        = read_data("model/dim_meters")
dim_suppliers     = read_data("model/dim_suppliers")
fct_electric_brew = read_data("model/fct_electric_brew")


'''
=========================================
=============== CURATION ================
=========================================

Contains utility functions that scrape and restructure data from raw sources into more structured formats.

Functions:
    - curate_meter_usage : Curates meter usage data from raw CSVs into partitioned Parquet files.
    - curate_locations   : Converts a CSV with manual entries into Parquet files.
    - curate_cmp_bills   : Curates the manuall edited CSVs from `1scrape_cmp_bills` into partitioned Parquet files.
    - scrape_cmp_bills   : Scrapes billing data from PDFs into a CSV file.

'''

def curate_meter_usage(raw           : str  = "./data/cmp/raw/meter-usage", 
                       curated       : str  = "./data/cmp/curated/meter-usage",
                       partition_col : list = ['account_number'],
                       schema        : list = ["account_number", "service_point_id", "meter_id", 
                                               "interval_end_datetime", "meter_channel", "kwh"]):
    '''
    This function reads all CSVs in the specified `raw` directory, adds the defined `schema`, and then saves
    the combined data as a partitioned .parquet file in the `curated` directory with snappy compression.
    
    Methodology:
        1. Read all CSVs in the `raw` directory without headers and assign the defined column names.
        2. Save the DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`.
        
    Parameters:
        raw           (str)  : Path to the directory containing raw `meter-usage` CSV files.
        curated       (str)  : Directory where the partitioned .parquet files should be saved.
        partition_col (list) : Column name(s) to use for partitioning the parquet files.
        schema        (list) : Column names(s) to apply to the resulting DataFrame as headers.
    '''
    
    try:
        # Step 1: Read all CSVs in the `raw` directory and add headers
        raw_files = [os.path.join(raw, file) for file in os.listdir(raw) if file.endswith('.csv')]
        if not raw_files:
            lg.warning(f"No CSV files found in {raw}. Exiting function.")
            return
    
        concat_df = pd.concat([pd.read_csv(file, 
                                           header  = None, 
                                           usecols = range(len(schema)),
                                           names   = schema) for file in raw_files])
        
        # Step 2: Save the DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`
        if not os.path.exists(curated):
            lg.warning(f"Directory {curated} does not exist. It will now be created.")
            os.makedirs(curated)

        pq.write_to_dataset(pa.Table.from_pandas(concat_df), 
                            root_path      = curated, 
                            partition_cols = partition_col, 
                            compression    = 'snappy', 
                            use_dictionary = True)
        
        lg.info(f"Data saved as partitioned .parquet files in {curated}.")

    except Exception as e:
        lg.error(f"Error while curating meter usage data: {e}")

def curate_locations(raw           : str  = "./data/cmp/raw/locations", 
                     curated       : str  = "./data/cmp/curated/locations",
                     partition_col : list = ['account_number']):
    '''
    This function reads all CSVs in the specified `raw` directory, uses the existing headers, and then saves
    the combined data as a partitioned .parquet file in the `curated` directory with snappy compression.
    
    Methodology:
        1. Read all CSVs in the `raw` directory using the existing headers.
        2. Save the DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`.
        
    Parameters:
        raw           (str)  : Path to the directory containing the raw `locations` CSV file.
        curated       (str)  : Directory where the partitioned .parquet files should be saved.
        partition_col (list) : Column name(s) to use for partitioning the parquet files.
    '''

    try:
        # Step 1: Read all CSVs in the `raw` directory using existing headers
        raw_files = [os.path.join(raw, file) for file in os.listdir(raw) if file.endswith('.csv')]
        if not raw_files:
            lg.warning(f"No CSV files found in {raw}. Exiting function.")
            return
    
        concat_df = pd.concat([pd.read_csv(file) for file in raw_files])
        
        # Step 2: Save the DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`
        if not os.path.exists(curated):
            lg.warning(f"Directory {curated} does not exist. It will now be created.")
            os.makedirs(curated)

        pq.write_to_dataset(pa.Table.from_pandas(concat_df), 
                            root_path      = curated, 
                            partition_cols = partition_col, 
                            compression    = 'snappy', 
                            use_dictionary = True)
        
        lg.info(f"Data saved as partitioned .parquet files in {curated}.")

    except Exception as e:
        lg.error(f"Error while curating cmp bills data: {e}")

def curate_cmp_bills(raw           : str  = "./data/cmp/raw/bills", 
                     curated       : str  = "./data/cmp/curated/bills",
                     partition_col : list = ['account_number']):
    '''
    This function reads all CSVs in the specified `raw` directory, uses the existing headers, and then saves
    the combined data as a partitioned .parquet file in the `curated` directory with snappy compression.
    
    Methodology:
        1. Read all CSVs in the `raw` directory using the existing headers.
        2. Save the DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`.
        
    Parameters:
        raw           (str)  : Path to the directory containing raw `bills` CSV files.
        curated       (str)  : Directory where the partitioned .parquet files should be saved.
        partition_col (list) : Column name(s) to use for partitioning the parquet files.
    '''

    try:
        # Step 1: Read all CSVs in the `raw` directory using existing headers
        raw_files = [os.path.join(raw, file) for file in os.listdir(raw) if file.endswith('.csv')]
        if not raw_files:
            lg.warning(f"No CSV files found in {raw}. Exiting function.")
            return
    
        concat_df = pd.concat([pd.read_csv(file) for file in raw_files])
        
        # Step 2: Save the DataFrame as a .parquet file in the `curated` directory, partitioned by `partition_col`
        if not os.path.exists(curated):
            lg.warning(f"Directory {curated} does not exist. It will now be created.")
            os.makedirs(curated)

        pq.write_to_dataset(pa.Table.from_pandas(concat_df), 
                            root_path      = curated, 
                            partition_cols = partition_col, 
                            compression    = 'snappy', 
                            use_dictionary = True)
        
        lg.info(f"Data saved as partitioned .parquet files in {curated}.")

    except Exception as e:
        lg.error(f"Error while curating cmp bills data: {e}")

def scrape_cmp_bills(raw    : str = "./data/cmp/raw/bills",
                     output : str = "./data/cmp/raw/bills"):
    '''
    This function reads all PDFs in the specified `raw` directory, extracts specific information from the 
    electricity bills using regular expressions, and then saves the consolidated data as a CSV file 
    in the `output` directory.
    
    Methodology:
        1. Iterate over each PDF in the `raw` directory.
        2. Extract the text content of each page in the PDF.
        3. Use regular expressions to scrape relevant billing and meter details.
        4. Append the scraped data to a list of dictionaries.
        5. Convert the list of dictionaries to a DataFrame.
        6. Save the DataFrame as a CSV file in the `output` directory.
        
    Parameters:
        raw    (str) : Path to the directory containing raw electricity bill PDF files.
        output (str) : Directory where the scraped data CSV file should be saved.
    '''
    
    try:
        records = []
        for pdf_path in glob(f"{raw}/**/*pdf", recursive = True):

            def extract_field(pattern, replace_dict = None):
                search_result = search(pattern, pdf_text)
                field_value   = search_result.group(1) if search_result else "NULL"
                
                if replace_dict:
                    field_value = "".join(replace_dict.get(char, char) for char in field_value)
                    
                return field_value

            with pl.open(pdf_path) as pdf:
                pdf_text = " ".join(page.extract_text() for page in pdf.pages)

            meter_details = search(r"Delivery Charges.*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,4},?\d{0,3}) KWH.*?Total Current Delivery Charges", 
                                    pdf_text, 
                                    DOTALL)
            
            records.append({'account_number' : extract_field(r"Account Number\s*([\d-]+)", {"-": ""}),
                            'supplier'       : "", # To be manually overwritten
                            'amount_due'     : extract_field(r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"),
                            'service_charge' : extract_field(r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})", {"$": "", "+": ""}),
                            'kwh_delivered'  : meter_details.group(3).replace(",", "") if meter_details else "NULL",
                            'delivery_rate'  : extract_field(r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"),
                            'supply_rate'    : "", # To be manually overwritten
                            'interval_start' : meter_details.group(1) if meter_details else "NULL",
                            'interval_end'   : meter_details.group(2) if meter_details else "NULL",
                            'total_kwh'      : "", # To be manually overwritten
                            'pdf_file_name'  : os.path.basename(pdf_path)})

        df = pd.DataFrame(records)
        
        if not os.path.exists(output):
            os.makedirs(output)
            
        df.to_csv(os.path.join(output, 'scraped_bills.csv'), index = False)
    
    except Exception as e:
        return f"Error while curating bill data: {e}"


'''
=========================================
=============== MODELING ================
=========================================

Contains functions that transform DATAFRAMES into a star schema optimized for analytical queries and 
data visualization. The aim is to create a structured, denormalized data model that enables fast and 
intuitive querying.

Functions:
    - create_dim_datetimes : Generates a datetime dimension table from `meter_usage` timestamps and saves as Parquet.
    - create_dim_accounts  : Extracts account, location, and meter dimensions and saves as Parquet.
'''

def create_dim_datetimes(model : str = "./data/model/dim_datetimes"):
    '''
    This function creates a datetime dimension table from the `meter_usage` DataFrame.
    It extracts unique timestamps, generates various time components, and saves the result as a .parquet file.
    
    Methodology:
        1. Extract unique timestamps from `meter_usage['interval_end_datetime']` and sort them.
        2. Create a DataFrame with these timestamps and generate time components such as increment, hour, etc.
        3. Define the period of the day based on the hour.
        4. Insert `id` at the first column position
        5. Save the DataFrame as a .parquet file in the specified `model` directory with snappy compression.
        
    Parameters:
        model (str) : Directory where the .parquet file should be saved.
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
        df['week_in_year'] = df['timestamp'].dt.isocalendar().week
        df['month']        = df['timestamp'].dt.month
        df['month_name']   = df['timestamp'].dt.month_name()
        df['quarter']      = df['timestamp'].dt.quarter
        df['year']         = df['timestamp'].dt.year

        # Step 3: Define the period based on the hour
        df['period'] = df['hour'].apply(
            lambda hour: 'Off-peak: 12AM to 7AM' if 0 <= hour < 7 else (
                         'Mid-peak: 7AM to 5PM, 9PM to 11PM' if (7 <= hour < 17) or (21 <= hour < 23) else 
                         'On-peak: 5PM to 9PM'))
        
        # Step 4: Insert `id` at the first column position
        df.insert(0, 'id', range(1, len(df) + 1))

        # Step 5: Save the DataFrame as a .parquet file
        pq.write_to_dataset(pa.Table.from_pandas(df), 
                            root_path      = model, 
                            compression    = 'snappy', 
                            use_dictionary = True)

        lg.info(f"Dimension table saved as .parquet file in {model}.")

    except Exception as e:
        lg.error(f"Error creating datetime dimension table: {e}")

def create_dim_meters(model : str = "./data/model/dim_meters"):
    '''
    This function creates a meters dimension table by joining data from the `meter_usage` and `locations` DataFrames.
    It extracts account numbers, service points, meter IDs, streets, and labels, and saves the result as a .parquet file.

    Methodology:
        1. Extract and join relevant columns based on `account_number`.
        2. Create a unique identifier `id` for each row.
        3. Save the resulting DataFrame as a .parquet file in the specified `model` directory with snappy compression.

    Parameters:
        model (str) : Directory where the .parquet file should be saved.
    '''

    try:
        # Step 1: Extract and join relevant columns
        df = pd.merge(meter_usage[['meter_id', 'service_point_id', 'account_number']].drop_duplicates(), 
                      locations[['account_number', 'street', 'label']].drop_duplicates(), 
                      on  = 'account_number', 
                      how = 'left')

        # Step 2: Create a unique identifier `id` for each row
        df.insert(0, 'id', range(1, len(df) + 1))

        # Step 3: Save the resulting DataFrame as a .parquet file
        pq.write_to_dataset(pa.Table.from_pandas(df), 
                            root_path      = model, 
                            compression    = 'snappy', 
                            use_dictionary = True)

        lg.info(f"Meters dimension table saved as .parquet file in {model}.")

    except Exception as e:
        lg.error(f"Error creating meters dimension table: {e}")

def create_dim_suppliers(model : str = "./data/model/dim_suppliers"):
    '''
    This function creates a suppliers dimension table from the `cmp_bills` DataFrame.
    It extracts the supplier name and calculates the average supply rate, then saves the result as a .parquet file.

    Methodology:
        1. Group `cmp_bills` by `supplier` and calculate the average `supply_rate`.
        2. Assign a unique identifier `id` for each row.
        3. Save the resulting DataFrame as a .parquet file in the specified `model` directory with snappy compression.
    
    Parameters:
        model (str) : Directory where the .parquet file should be saved.
    '''

    try:
        # Step 1: Group by `supplier` and calculate average `supply_rate`
        df = cmp_bills.groupby('supplier', as_index = False).agg(avg_supply_rate = ('supply_rate', 'mean'))
        
        # Step 2: Assign unique identifier `id`
        df.insert(0, 'id', range(1, len(df) + 1))

        # Step 3: Save the DataFrame as a .parquet file
        pq.write_to_dataset(pa.Table.from_pandas(df), 
                            root_path      = model, 
                            compression    = 'snappy', 
                            use_dictionary = True)

        lg.info(f"Suppliers dimension table saved as .parquet file in {model}.")

    except Exception as e:
        lg.error(f"Error creating suppliers dimension table: {e}")

def create_fct_eletric_brew(model         : str  = "./data/model/fct_electric_brew",
                            partition_col : list = ['account_number']):
    
    '''
    This function generates the central fact table which records the electric usage and associated charges for each
    account per time interval. This table is the cornerstone of our analytics model, enabling detailed insights into 
    usage patterns, billing calculations, and the financial implications of electric delivery.

    The final result integrates data from meter readings, customer billing, and rate information, applying business
    rules to calculate the cost of electric delivery and usage. The calculated fields include allocated service charges
    based on usage, the cost of electric delivery, and the used kilowatt-hours (kWh). These metrics are critical for 
    understanding electric consumption and financials at a granular level.

    Methodology:
        1. Expand 'cmp_bills' to create a daily granularity level based on the billing interval.
        2. Curate an intermediary DataFrame by merging meter usage data with dimension tables and billing data.
        3. Calculate cumulative and usage-based metrics such as 'allocated_service_charge', 'delivered_kwh_left', and 'delivered_kwh_used'.
        4. Determine the 'total_cost_of_delivery' by summing up the delivery and supply rates along with an allocated service charge.
        5. Assign a unique identifier 'id' for each row.
        6. Save the resulting DataFrame as a .parquet file in the specified 'model' directory with snappy compression.

    Parameters:
        model (str): Directory where the .parquet file should be saved.
        partition_col (list): List of column names to partition the .parquet file upon.
    '''
    
    try:
        # Step 1: Expand 'cmp_bills' data to daily granularity
        exploded_bills = cmp_bills.assign(date = lambda df: 
                                  df.apply(lambda row: pd.date_range(start = row['interval_start'], 
                                                                     end   = row['interval_end'])
                                                         .to_list(), axis = 1)) \
                                                         .explode('date')

        # Step 2: Merge and curate intermediary DataFrame
        int_df = meter_usage.drop('account_number', axis = 1) \
                            .assign(timestamp = lambda df: pd.to_datetime(df['interval_end_datetime'], 
                                                                          format = '%m/%d/%Y %I:%M:%S %p')) \
                            .merge(dim_meters,     on = 'meter_id',  how = 'left').rename(columns = {'id' : 'dim_meters_id'}) \
                            .merge(dim_datetimes,  on = 'timestamp', how = 'left').rename(columns = {'id' : 'dim_datetimes_id'}) \
                            .merge(exploded_bills, on = ['account_number', 'date'], how = 'left') \
                            .merge(dim_suppliers,  on = 'supplier',  how = 'left').rename(columns = {'id' : 'dim_suppliers_id'}) \
                            .sort_values(by = ['pdf_file_name', 'timestamp']) \
                            .fillna({'kwh_delivered'  : 0,
                                     'service_charge' : 0,
                                     'delivery_rate'  : 0, 
                                     'supply_rate'    : 0})

        # Steps 3 & 4: Calculate metrics and total cost of delivery
        fct_df = (
            int_df.assign(
                total_recorded_kwh       = int_df.groupby(['pdf_file_name', 'kwh_delivered'])['kwh'].transform('sum'),
                allocated_service_charge = lambda x: x['service_charge'] * x['kwh'] / x['total_recorded_kwh'],
                delivered_kwh_left       = int_df.groupby(['pdf_file_name', 'kwh_delivered'], group_keys = False)
                                                 .apply(lambda g: (g['kwh_delivered'].iloc[0] - g['kwh'].iloc[::-1].cumsum())
                                                 .clip(lower = 0)),
                delivered_kwh_used       = lambda x: np.minimum(x['kwh'], x['delivered_kwh_left']).clip(lower = 0),
                total_cost_of_delivery   = lambda x: x['delivered_kwh_used'] * (x['delivery_rate'] + x['supply_rate']) +
                                                     x['allocated_service_charge'])) \
                .sort_values(by = ['dim_meters_id', 'dim_datetimes_id']) \
                [[
                    # Relational and partition keys
                    'dim_datetimes_id',
                    'dim_meters_id',
                    'dim_suppliers_id',
                    'account_number',

                    # Established facts from `meter_usage` and `cmp_bills`
                    'kwh',
                    'service_charge',
                    'delivery_rate',
                    'supply_rate',

                    # Newly curated facts
                    'allocated_service_charge',
                    'delivered_kwh_left',
                    'delivered_kwh_used',
                    'total_cost_of_delivery'
                ]]
        
        # Step 5: Create a unique identifier `id` for each row
        fct_df.insert(0, 'id', range(1, len(fct_df) + 1))

        # Step 6: Save the resulting DataFrame as a .parquet file
        pq.write_to_dataset(pa.Table.from_pandas(fct_df), 
                            root_path      = model, 
                            partition_cols = partition_col, 
                            compression    = 'snappy', 
                            use_dictionary = True)

        lg.info(f"Final fact table saved as .parquet file in {model}.")

    except Exception as e:
        lg.error(f"Error while creating the final fact table: {e}")