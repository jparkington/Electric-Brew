from matplotlib.pyplot import rcParams

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
    - set_plot_params   : Sets up custom plot parameters for matplotlib.
    - find_project_root : Finds the project directory by searching for a specified identifier in the directory tree.
    - read_data         : Reads a .parquet file into a Pandas DataFrame.
    - connect_to_db     : Connects to DuckDB and creates specified views within it if not already present.
'''

def setup_plot_params():
    '''
    This function sets up custom plot parameters for matplotlib plots.
    
    The function defines various parameters related to axes, figure, font, text, grid, and legend of the plot.
    These parameters are then used to update matplotlib's runtime configuration, ensuring all subsequent plots
    adhere to this custom style.
    '''

    rcParams.update({# Axes parameters                           # Tick parameters
                    'axes.facecolor'     : '.05',                'xtick.labelsize'    : 8,
                    'axes.grid'          : True,                 'xtick.color'        : '1',
                    'axes.labelcolor'    : 'white',              'xtick.major.size'   : 0,
                    'axes.spines.left'   : False,                'ytick.labelsize'    : 8,
                    'axes.spines.right'  : False,                'ytick.color'        : '1',
                    'axes.spines.top'    : False,                'ytick.major.size'   : 0,
                    'axes.labelsize'     : 10,
                    'axes.labelweight'   : 'bold',               # Figure parameters
                    'axes.titlesize'     : 13,                   'figure.facecolor'   : 'black',
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
                    'grid.color'         : '0.2'})

def find_project_root(root_id : str = '.git') -> str:
    '''
    Finds the project root directory by searching for a specified identifier in the directory tree.

    This function starts at the directory of the script that calls it, and iteratively moves up the directory tree 
    until it finds a directory containing the specified identifier, usually a unique file or directory like '.git'. 
    This is useful for determining the project root directory in a dynamic and reliable way, regardless of the 
    specific location of the script within the project.

    Parameters:
        root_identifier (str) : A unique identifier that signifies the project root directory. 
                                Defaults to '.git', which is common for Git repositories.

    Returns:
        str : The absolute path to the project root directory.
    '''

    try:
        current   = os.path.abspath(__file__) # Get the absolute path of the current file
        directory = os.path.dirname(current)  # Get the directory of the current file

        project_root = directory
        while not os.path.exists(os.path.join(project_root, root_id)):

            project_root = os.path.dirname(project_root)

            if project_root == '/':  # Safety check to avoid infinite loop
                lg.error(f"Project root with identifier '{root_id}' not found.")
            
        return project_root

    except Exception as e:
        lg.error(f"Error finding project root: {e}\n")


def read_data(file_path: str) -> pd.DataFrame:
    '''
    Reads a .parquet file from a specified relative path into a Pandas DataFrame.
    The function automatically resolves the path relative to the project's /data/ directory.

    Parameters:
        file_path (str): Relative path to the .parquet file, starting from the /data/ directory.
        
    Returns:
        pd.DataFrame: DataFrame containing the data read from the .parquet file.
    '''

    # Combine the project root with `data`` directory and the file path
    full_file_path = os.path.join(find_project_root(), 'data', file_path)

    # Read the .parquet file and return as a Pandas DataFrame
    return pq.read_table(full_file_path).to_pandas()

def connect_to_db(path : str = './data/sql/electric_brew.db',
                  vws  : dict = {'meter_usage'       : 'cmp/curated/meter_usage',
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

    db = dd.connect(

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