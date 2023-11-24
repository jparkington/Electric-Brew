from matplotlib.pyplot import rcParams
from seaborn           import set_style

import os
import duckdb          as dd
import logging         as lg
import pandas          as pd
import pyarrow.parquet as pq

lg.basicConfig(level  = lg.INFO, 
               format = '%(asctime)s | %(levelname)s | %(message)s')

'''
Contains utility functions that configure the runtime environment and are called as scripts are
executed. These include `rcParams` and specific paradigms for reading data into dataframes.

Functions:
    - set_plot_params : Sets up custom plot parameters for matplotlib.
    - read_data       : Reads a .parquet file into a Pandas DataFrame.
    - connect_to_db   : Connects to DuckDB and creates specified views within it if not already present.
'''

def setup_plot_params():
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
    1. Get the absolute path of the current file (`runtime.py`) using `os.path.abspath(__file__)`.
    2. Determine the directory of the current file (`src`) using `os.path.dirname()`.
    3. Navigate to the project root directory by going up one level with `..`.
    4. Append 'data' to the project root directory to reach the /data/ directory.
    5. Join this with the user-provided `file_path` to construct the full path to the .parquet file.
    
    Parameters:
        file_path (str) : Relative path to the .parquet file, starting from the /data/ directory.
        
    Returns:
        pd.DataFrame : DataFrame containing the data read from the .parquet file.
    '''
    
    current_file_path = os.path.abspath(__file__)               # Get the absolute path of the current file
    src_directory     = os.path.dirname(current_file_path)      # Get the directory of the file at runtime
    project_root      = os.path.join(src_directory, '..', '..') # Go up two directories to get to the project root
    data_directory    = os.path.join(project_root, 'data')      # Join with 'data' to get to the data directory
    full_file_path    = os.path.abspath(os.path.join(data_directory, file_path))

    df = pq.read_table(full_file_path).to_pandas()

    return df

def connect_to_db(db  : dd.DuckDBPyConnection = dd.connect('./data/sql/electric_brew.db'),
                  vws : dict = {'meter_usage'       : 'cmp/curated/meter_usage',
                                'locations'         : 'cmp/curated/locations',
                                'cmp_bills'         : 'cmp/curated/bills',
                                'ampion_bills'      : 'ampion/curated',
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
            lg.error(f"Error occurred while creating view '{k}': {e}\n")
            raise e

    return db