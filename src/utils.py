from datetime          import datetime
from glob              import glob
from matplotlib.pyplot import rcParams
from re                import findall, search, DOTALL
from seaborn           import set_style
from shutil            import rmtree
from typing            import *

import os
import duckdb          as dd
import logging         as lg
import numpy           as np
import pandas          as pd
import pdfplumber      as pl
import pyarrow         as pa
import pyarrow.parquet as pq

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
    - connect_to_db   : Connects to DuckDB and creates specified views within it if not already present.
'''

def setup_plot_params() -> List:
    '''
    This function sets up custom plot parameters for matplotlib plots.
    
    The function defines various parameters related to axes, figure, font, text, grid, and legend of the plot.
    These parameters are then used to update matplotlib's runtime configuration, ensuring all subsequent plots
    adhere to this custom style.
    '''

    # Use seaborn's darkgrid style
    set_style('darkgrid')

    rcParams.update({# Axes parameters                           # Tick parameters
                     'xtick.labelsize'    : 8,                   'ytick.labelsize'    : 8,
                     'axes.labelsize'     : 10,
                     'axes.labelweight'   : 'bold',              # Figure parameters
                     'axes.titlesize'     : 13,                  'figure.figsize'     : (15, 10),
                     'axes.titleweight'   : 'bold',              'figure.autolayout'  : True,
                     'axes.labelpad'      : 15,
                     'axes.titlepad'      : 15,

                     # Font and text parameters                  # Legend parameters
                     'font.family'        : 'DejaVu Sans Mono',  'legend.borderpad'   : 0.75,
                     'font.size'          : 8,                   'legend.facecolor'   : '1',
                     'font.style'         : 'normal',            

                     # Grid parameters
                     'grid.linestyle'     : ':'})

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

    return df

def connect_to_db(db  : dd.DuckDBPyConnection = dd.connect('./data/sql/electric_brew.db'),
                  vws : dict = {'meter_usage'       : 'cmp/curated/meter_usage',
                                'locations'         : 'cmp/curated/locations',
                                'cmp_bills'         : 'cmp/curated/bills',
                                'ampion_bills'      : 'ampion/curated/bills',
                                'dim_datetimes'     : 'modeled/dim_datetimes',
                                'dim_meters'        : 'modeled/dim_meters',
                                'dim_bills'         : 'modeled/dim_bills',
                                'fct_electric_brew' : 'modeled/fct_electric_brew'}) -> dd.DuckDBPyConnection:
    '''
    This function creates views in a DuckDB database for the Electric Brew project by reading data from 
    parquet files. It leverages DuckDB's ability to directly query Parquet files, which simplifies the 
    data loading process compared to a traditional SQL database approach.

    These views will automatically change as the underlying Parquet data changes. If the view already exists
    at runtime, this function will continue past the view creation step.

    Methodology:
        1. Establish a connection to the DuckDB database, or create it if it doesn't exist.
        2. Loop through the provided view names and corresponding Parquet file paths.
        3. For each view, create a SQL view in the database that reads data directly from parquet files.

    Parameters:
        db  : Path and connection to the DuckDB database.
        vws : Dictionary mapping view names to corresponding Parquet file paths.

    Returns:
        duckdb.DuckDBPyConnection: The connected DuckDB database instance.
    '''

    for k, v in vws.items():
        try:
            # Create SQL view for each parquet file
            db.execute(f"CREATE VIEW {k} AS SELECT * FROM read_parquet('./data/{v}/**/*.parquet')")
            lg.info(f"Created view '{k}' from parquet files at '{v}'.")

        except dd.duckdb.CatalogException:
            # This error occurs if the view already exists
            continue

        except Exception as e:
            lg.error(f"Error occurred while creating view '{k}': {e}")
            raise e

    return db

electric_brew = connect_to_db()

'''
=========================================
============== DATAFRAMES ===============
=========================================

Contains commonly used DataFrames initialized at the start for easier access across different scripts. 
These DataFrames are curated and optimized for efficient data operations.

DataFrames:
    - meter_usage       : Contains kWh readings from CMP in as frequent as 15-minute intervals.
    - locations         : Adds manual CSV entries describing each of the accounts Austin St. uses to the model.
    - cmp_bills         : Contains billed delivery and supplier rates for various periods of activity.
    - ampion_bills      : Contains pricing and kWhs supplied by Austin Street's solar provider.
    - dim_datetimes     : Breaks down timestamps into individual date and time components, with categorization of periods.
    - dim_meters        : Abstracts the account numbers, service points, meter IDs, and streets dimensions into one table.
    - dim_bills         : Unions common dimensions and numerics from the `cmp_bills` and `ampion_bills` DataFrames
    - fct_electric_brew : Houses all the model's facts about usage, billing, and the cost of delivery.
'''

# Curated sources
meter_usage  = read_data("cmp/curated/meter_usage")
locations    = read_data("cmp/curated/locations")
cmp_bills    = read_data("cmp/curated/bills")
ampion_bills = read_data("ampion/curated/bills")

# Model
dim_datetimes     = read_data("modeled/dim_datetimes")
dim_meters        = read_data("modeled/dim_meters")
dim_bills         = read_data("modeled/dim_bills")
fct_electric_brew = read_data("modeled/fct_electric_brew")


'''
=========================================
=============== CURATION ================
=========================================

Contains utility functions that scrape and restructure data from raw sources into columnar, efficient formats.

The first two utility functions are used in `curation.py` to process and generate the following DataFrames without 
additional transformations needed:
    - meter_usage
    - locations
    - cmp_bills
    - ampion_bills

Functions:
    - load_data_files     : Load data files from the specified directory. Supports CSV and PDF file types.
    - write_results       : Write curated data to a specified Parquet directory.
    - scrape_cmp_bills    : 
    - scrape_ampion_bills :
'''

def load_data_files(path : str, 
                    type : str = 'CSV', 
                    cols : List[str] = None) -> Union[pd.DataFrame, str]:
    '''
    Load data files from the specified directory. Supports CSV and PDF file types.

    Methodology:
        1. Determine the file type (CSV, PDF, Parquet) and prepare to load files accordingly.
        2. For CSV files:
           a. Load each file, optionally applying specified column names.
           b. Concatenate all CSV data into a single DataFrame.
        3. For PDF files:
           a. Extract text content from each page of each PDF file.
           b. Create a DataFrame with content and page number for each extracted page.
        4. For Parquet files:
           a. Directly read the Parquet dataset from the specified directory.

    Parameters:
        path (str)       : Path to the directory containing the files.
        type (str)       : Type of the files to load (CSV, PDF, Parquet). Defaults to 'CSV'.
        cols (List[str]) : List of column names to be used as headers for CSV files. Defaults to None.

    Returns:
        pd.DataFrame : Loaded data as a DataFrame. For CSV and Parquet files, it concatenates all data;
                       for PDF files, each row represents a page's content and page number.
    '''

    # Step 1: Load all files for a given data `type`.
    type  = type.lower()
    files = glob(os.path.join(path, "**", f"*.{type}"), recursive = True)

    if not files:
        raise FileNotFoundError(f"No '{type}' files found in {path}.")
    
    try:
        # Step 2: For CSV files
        if type == 'csv':

            lg.info(f"Loading CSV files from {path}.")
            csvs = [pd.read_csv(file, names = cols, header = None) if cols else pd.read_csv(file) 
                    for file in files]
            
            return pd.concat(csvs, ignore_index = True)

        # Step 3: For PDF files
        elif type == 'pdf':

            lg.info(f"Loading PDF files from {path}.")
            pages = [{'page_number': page.page_number, 'file_path': file, 'content': page.extract_text()}
                     for file in files
                     for page in pl.open(file).pages
                     if page.extract_text()]
            
            return pd.DataFrame(pages)
        
        # Step 4: For Parquet files
        elif type == 'parquet':

            lg.info(f"Loading Parquet dataset from {path}.")

            return pq.read_table(path).to_pandas()

        else:
            raise ValueError(f"Unsupported file type: {type}")

    except Exception as e:
        lg.error(f"Error loading files from {path}: {e}")

def write_results(data           : pd.DataFrame, 
                  dest           : str, 
                  add_id         : bool = False, 
                  partition_by   : str  = 'account_number',
                  compression    : str  = 'snappy',
                  use_dictionary : bool = True,
                  overwrite      : bool = True):
    '''
    Write curated data to a specified Parquet directory with an optional primary key, optional partitioning,
    and snappy compression. Optionally overwrite existing data or append to it.

    Methodology:
        1. Check if the destination directory exists. If not, create it.
        2. If the directory exists and `overwrite` is True, delete the existing data and recreate the directory.
        3. If the directory exists and `overwrite` is False, prepare to append data to the existing directory.
        4. Optionally add a unique identifier to the data.
        5. Write the DataFrame to the specified Parquet destination, handling compression and partitioning if required.

    Parameters:
        data           (pd.DataFrame) : The DataFrame to be written.
        dest           (str)          : Path to the destination directory.
        add_id         (bool)         : Whether to add a unique identifier to the data. Defaults to False.
        partition_by   (str)          : Column to partition by. Defaults to 'account_number'.
        compression    (str)          : Compression method for Parquet files. Defaults to 'snappy'.
        use_dictionary (bool)         : Whether to enable dictionary encoding. Defaults to True.
        overwrite      (bool)         : Whether to overwrite existing data in the directory. Defaults to True.
    '''
    
    # Step 1: Check if the destination directory exists.
    if not os.path.exists(dest):
        lg.info(f"Directory {dest} does not exist. It will now be created.")

    # Step 2: Delete the existing data and recreate the directory.
    elif overwrite:
        lg.info(f"Overwriting existing data in directory {dest}.")
        rmtree(dest)

    # Step 3: Append data to the existing directory.
    else:
        lg.info(f"Directory {dest} exists and `overwrite` is not set. Data will be appended instead.")

    os.makedirs(dest, exist_ok = True)
    
    # Step 4: Optionally add a unique identifier to the data.
    if add_id:
        data['id'] = range(1, len(data) + 1)

    # Step 5: Write the DataFrame to the specified Parquet destination.
    try:
        pq.write_to_dataset(pa.Table.from_pandas(data), 
                            root_path      = dest, 
                            partition_cols = [partition_by] if partition_by else None,
                            compression    = compression,
                            use_dictionary = use_dictionary)

        lg.info(f"Data written in Parquet to {dest}.")

    except Exception as e:
        lg.error(f"Error writing data to {dest}: {e}")

# def scrape_cmp_bills(raw    : str = "./data/cmp/raw/bills",
#                      output : str = "./data/cmp/raw/bills"):
#     '''
#     This function reads all PDFs in the specified `raw` directory, extracts specific information from the 
#     electricity bills using regular expressions, and then saves the consolidated data as a CSV file 
#     in the `output` directory.
    
#     Methodology:
#         1. Iterate over each PDF in the `raw` directory.
#         2. Extract the text content of each page in the PDF.
#         3. Use regular expressions to scrape relevant billing and meter details.
#         4. Append the scraped data to a list of dictionaries.
#         5. Convert the list of dictionaries to a DataFrame.
#         6. Save the DataFrame as a CSV file in the `output` directory.
        
#     Parameters:
#         raw    (str) : Path to the directory containing raw electricity bill PDF files.
#         output (str) : Directory where the scraped data CSV file should be saved.
#     '''
    
#     try:
#         # Step 1: Iterate through each PDF in the `raw` directory
#         pdf_data = load_data_files(path = raw, type = 'PDF')

#             def extract_field(pattern, replace_dict = None):
#                 search_result = search(pattern, pdf_text)
#                 field_value   = search_result.group(1) if search_result else "NULL"
                
#                 if replace_dict:
#                     field_value = "".join(replace_dict.get(char, char) for char in field_value)
                    
#                 return field_value

#             meter_details = search(r"Delivery Charges.*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,4},?\d{0,3}) KWH.*?Total Current Delivery Charges", 
#                                     pdf_text, 
#                                     DOTALL)
            
#             records.append({'invoice_number' : os.path.basename(pdf_path).split('_')[0],
#                             'account_number' : extract_field(r"Account Number\s*([\d-]+)", {"-": ""}),
#                             'supplier'       : "",
#                             'amount_due'     : extract_field(r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"),
#                             'service_charge' : extract_field(r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})", {"$": "", "+": ""}),
#                             'kwh_delivered'  : meter_details.group(3).replace(",", "") if meter_details else "NULL",
#                             'delivery_rate'  : extract_field(r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"),
#                             'supply_rate'    : "",
#                             'interval_start' : datetime.strptime(meter_details.group(1), "%m.%d.%Y").strftime("%Y-%m-%d") if meter_details else "NULL",
#                             'interval_end'   : datetime.strptime(meter_details.group(2), "%m.%d.%Y").strftime("%Y-%m-%d") if meter_details else "NULL",
#                             'total_kwh'      : ""})

#         write_results(data = pd.DataFrame(records), dest = output)
    
#     except Exception as e:
#         return f"Error while curating bill data: {e}"
    
def scrape_ampion_bills(raw    : str = "./data/ampion/raw/bills/pdf", 
                        output : str = "./data/ampion/raw/bills/csv"):
    '''
    This function reads all PDFs in the specified `raw` directory, extracts specific information from the 
    Ampion bills using regular expressions, and then saves a Parquet directory to the specified `output`.

    Methodology:
        1. Load data from PDF files in the `raw` directory using `load_data_files`.
        2. Create a map of the bills' abbreviated account numbers to full account numbers.
        3. Use regular expressions to find specific data fields in the extracted text.
        4. Create a list of dictionaries containing the scraped data, including "Miscellaneous Charges" if present.
        5. Write the data to CSV files in the `output` directory.

    The function handles standard charges as well as a conditional case for "Miscellaneous Charges" 
    which requires a different extraction logic for certain fields.

    Parameters:
        raw    (str): Path to the directory containing raw Ampion bill PDF files.
        output (str): Directory where the scraped data CSV files should be saved.
    '''

    try:
        # Step 1: Load data from PDF files
        pdf_data = load_data_files(path = raw, 
                                   type = 'PDF')

        # Step 2: Map abbreviated account numbers to full account numbers
        acc_map    = {str(acc)[-4:]: acc for acc in locations['account_number']}

        # Regular expressions for data fields
        r_invoice  = r"Invoice:\s(\d+)"
        r_abbr_acc = r'\*{5}(\d+)'
        r_dates    = r'(\d{2}\.\d{2}\.\d{4})\s*–\s*(\d{2}\.\d{2}\.\d{4})'
        r_kwh      = r'(\d{1,4}(?:,\d{3})*?) kWh'
        r_prices   = r'allocated\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})'
        records    = []

        # Step 3: Use regular expressions to extract data
        for _, row in pdf_data.iterrows():

            invoice_number = search(r_invoice,   row['content']).group(1)
            abbr_numbers   = findall(r_abbr_acc, row['content'])
            dates          = findall(r_dates,    row['content'])
            kwh_values     = findall(r_kwh,      row['content'])
            prices         = findall(r_prices,   row['content'])

             # Step 4: Create records for regular charges
            for i, abbr_number in enumerate(abbr_numbers):

                records.append({'invoice_number' : invoice_number,
                                'account_number' : acc_map.get(abbr_number[-4:], abbr_number),
                                'supplier'       : "Ampion",
                                'interval_start' : datetime.strptime(dates[i][0], "%m.%d.%Y").strftime("%Y-%m-%d"),
                                'interval_end'   : datetime.strptime(dates[i][1], "%m.%d.%Y").strftime("%Y-%m-%d"),
                                'kwh'            : int(kwh_values[i].replace(',', '')),
                                'bill_credits'   : prices[i][0],
                                'price'          : prices[i][1] if int(invoice_number[0:4]) < 2023 else prices[i][2]})
                
            if "Miscellaneous Charges" in row['content']:

                # Slice the content to only include text after "Miscellaneous Charges"
                misc_content = row['content'][row['content'].find("Miscellaneous Charges"):]
                    
                r_misc_abbr_acc = r"utility acct \*\*\*\*(\d+):"
                r_misc_kwh      = r"\*{4}(\d+):(\d+)\s*kWh"
                r_misc_credits  = r"\$(\d+(?:,\d{3})*\.\d{2})\s*bill credits"
                r_misc_prices   = r'bill credits allocated @ \$\s*(\d+(?:,\d{3})*\.\d{2})\s+\$\s*(\d+(?:,\d{3})*\.\d{2})'

                misc_abbr_number  = search(r_misc_abbr_acc, misc_content).group(1)
                misc_kwh          = search(r_misc_kwh,      misc_content).group(2)
                misc_bill_credits = search(r_misc_credits,  misc_content).group(1)
                misc_prices       = search(r_misc_prices,   misc_content).groups()

                 # Step 4 (cont.): Create records for "Miscellaneous Charges" if present
                records.append({'invoice_number' : invoice_number,
                                'account_number' : acc_map.get(misc_abbr_number, misc_abbr_number),
                                'supplier'       : "Ampion",
                                'interval_start' : datetime.strptime(dates[0][0], "%m.%d.%Y").strftime("%Y-%m-%d"),
                                'interval_end'   : datetime.strptime(dates[0][1], "%m.%d.%Y").strftime("%Y-%m-%d"),
                                'kwh'            : misc_kwh,
                                'bill_credits'   : misc_bill_credits,
                                'price'          : misc_prices[0] if int(invoice_number[0:4]) < 2023 else misc_prices[1]})

        # Step 5: Write the data to Parquet
        write_results(data = pd.DataFrame(records), 
                      dest = output)

    except Exception as e:
        print(f"Error while processing and exporting data: {e}")


'''
=========================================
=============== MODELING ================
=========================================

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
        
        # Step 4: Save the DataFrame as a .parquet file
        write_results(data         = df, 
                      dest         = model,
                      add_id       = True,
                      partition_by = None)

    except Exception as e:
        lg.error(f"Error creating datetime dimension table: {e}")

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
        lg.error(f"Error creating meters dimension table: {e}")

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
        df1 = cmp_bills.groupby(common_dims, observed = True) \
                       .agg(kwh_delivered  = ('kwh_delivered',  'sum'),
                            service_charge = ('service_charge', 'sum'), 
                            delivery_rate  = ('delivery_rate',  'mean'),
                            supply_rate    = ('supply_rate',    'mean')) \
                       .reset_index()
        df1['source'] = "CMP"

        # Standardize `ampion_bills`
        df2 = ampion_bills.groupby(common_dims, observed = True) \
                          .agg(kwh_delivered  = ('kwh', 'sum'), 
                               price          = ('price', 'sum')) \
                          .reset_index()
        df2['service_charge'] = 0
        df2['delivery_rate']  = 0
        df2['supply_rate']    = df2['price'] / df2['kwh_delivered']
        df2['source']         = "Ampion"
        df2.drop(columns = ['price'], inplace = True)

        # Step 2: Concatenate standardize dataframes
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
        lg.error(f"Error creating bills dimension table: {e}")

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
        6. Calculate delivery, service, and supply costs based on used kWh.
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

        # Steps 4 & 5: Calculate remaining and used kWh for CMP and Ampion
        cmp_waterfall = int_df.sort_values(by = ['invoice_number', 'timestamp'])
        int_df['delivered_kwh_left'] = cmp_waterfall.groupby(['invoice_number', 'kwh_delivered'], group_keys = False) \
                                                    .apply(lambda g: (g['kwh_delivered'].iloc[0] - g['kwh'].iloc[::-1].cumsum()).clip(lower = 0))
        int_df['delivered_kwh_used'] = np.minimum(int_df['kwh'], int_df['delivered_kwh_left']).clip(lower = 0)

        ampion_waterfall = int_df.sort_values(by = ['invoice_number_amp', 'timestamp'])
        int_df['ampion_kwh_left'] = ampion_waterfall.groupby(['invoice_number_amp', 'kwh_delivered_amp'], group_keys = False) \
                                                    .apply(lambda g: (g['kwh_delivered_amp'].iloc[0] - g['kwh'].cumsum()).clip(lower = 0))
        int_df['ampion_kwh_used'] = np.minimum(int_df['kwh'], int_df['ampion_kwh_left']).clip(lower = 0)

        # Step 6: Compute cost metrics
        df = pd.DataFrame(index = int_df.index)
        df['dim_datetimes_id']  = int_df['id']
        df['dim_meters_id']     = int_df['id_met']
        df['dim_bills_id']      = np.where(int_df['ampion_kwh_used'] > 0, int_df['id_amp'], int_df['id_cmp'])
        df['account_number']    = int_df['account_number']
        df['kwh']               = int_df['kwh']
        df['delivery_cost']     = int_df['delivered_kwh_used'] * int_df['delivery_rate']
        df['service_cost']      = int_df['service_charge']     * int_df['kwh'] / int_df['total_recorded_kwh']
        df['supply_cost']       = int_df['delivered_kwh_used'] * int_df['supply_rate'] + int_df['ampion_kwh_used'] * int_df['supply_rate_amp']
        df['total_cost']        = df['delivery_cost'] + df['service_cost'] + df['supply_cost']

        # Step 7: Save the DataFrame as a .parquet file
        write_results(data   = df, 
                      dest   = model,
                      add_id = True)

    except Exception as e:
        lg.error(f"Error while creating the final fact table: {e}")