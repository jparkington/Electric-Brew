from cycler            import cycler
from glob              import glob
from matplotlib.pyplot import rcParams
from seaborn           import color_palette
from re                import search, DOTALL

import os
import logging         as lg
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

This section contains utility functions that configure the runtime environment.

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
                    'axes.titleweight'   : 'bold',               'figure.figsize'     : (10, 7),
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

def read_data(file_path : str) -> pd.DataFrame:
    '''
    This function reads a .parquet file from the specified file path into a Pandas DataFrame.
    
    Parameters:
        file_path (str) : Relative path to the .parquet file.
        
    Returns:
        pd.DataFrame : DataFrame containing the data.
    '''

    return pq.read_table(file_path).to_pandas()


'''
=========================================
============== DATAFRAMES ===============
=========================================

This section contains commonly used DataFrames initialized at the start for easier access across different scripts.

DataFrames:
    - cmp_meter_usage : DataFrame containing meter usage data from CMP.
'''

# meter_usage = read_data("..data/cmp/curated/meter-usage")


'''
=========================================
=============== CURATION ================
=========================================

This section contains utility functions that curate data from raw sources into more structured formats.

Functions:
    - curate_meter_usage : Curates meter usage data from raw CSVs into partitioned Parquet files.
    - scrape_bills       : Scrapes billing data from PDFs into a CSV file.

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

        pq.write_to_dataset(pa.Table.from_pandas(concat_df, 
                                                 preserve_index = False), 
                            root_path      = curated, 
                            partition_cols = partition_col, 
                            compression    = 'snappy', 
                            use_dictionary = True)
        
        lg.info(f"Data saved as partitioned .parquet files in {curated}.")

    except Exception as e:
        lg.error(f"Error while curating meter usage data: {e}")

def scrape_bills(raw    : str = "./data/cmp/raw/bills",
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
            
            records.append({'account_number'        : extract_field(r"Account Number\s*([\d-]+)", {"-": ""}),
                            'amount_due'            : extract_field(r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"),
                            'service_charge'        : extract_field(r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})", {"$": "", "+": ""}),
                            'delivery_service_rate' : extract_field(r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"),
                            'read_date'             : meter_details.group(1) if meter_details else "NULL",
                            'prior_read_date'       : meter_details.group(2) if meter_details else "NULL",
                            'kwh_delivered'         : meter_details.group(3).replace(",", "") if meter_details else "NULL",
                            'pdf_file_name'         : os.path.basename(pdf_path)})

        df = pd.DataFrame(records)
        
        if not os.path.exists(output):
            os.makedirs(output)
            
        df.to_csv(os.path.join(output, 'scraped_bills.csv'), index = False)
    
    except Exception as e:
        return f"Error while curating bill data: {e}"
