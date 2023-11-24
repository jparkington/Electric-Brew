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

def find_project_root(rel_path : str = None,
                      root_id  : str = '.git',) -> str:
    '''
    Finds the project root directory by searching for a specified identifier in the directory tree.

    This function starts at the directory of the script that calls it, and iteratively moves up the directory tree 
    until it finds a directory containing the specified identifier, usually a unique file or directory like '.git'. 
    This is useful for determining the project root directory in a dynamic and reliable way, regardless of the 
    specific location of the script within the project.

    If a relative path is provided, it is appended to the project root using `os`

    Parameters:
        rel_path (str) : A relative path to append to the project root.
        root_id  (str) : A unique pattern that signifies the project root directory. Defaults to '.git', common for repos.


    Returns:
        str: The absolute path to the project root directory.
    '''

    try:
        current   = os.path.abspath(__file__) # Get the absolute path of the current file
        directory = os.path.dirname(current)  # Get the directory of the current file

        root = directory
        while not os.path.exists(os.path.join(root, root_id)):

            root = os.path.dirname(root)

            if root == '/':  # Safety check to avoid infinite loop
                lg.error(f"Project root with identifier '{root_id}' not found.")
            
        if rel_path:
            # Ignore the leading '.' if present in the relative path
            components = rel_path.lstrip('./').split(os.sep)
            root       = os.path.join(root, *components)

        return root

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

    # Read the .parquet file and return as a Pandas DataFrame
    return pq.read_table(find_project_root(file_path)).to_pandas()

def connect_to_db(path : str = './data/sql/electric_brew.db',
                  vws  : dict = {'meter_usage'       : './data/cmp/curated/meter_usage',
                                 'locations'         : './data/cmp/curated/locations',
                                 'cmp_bills'         : './data/cmp/curated/bills',
                                 'ampion_bills'      : './data/ampion/curated',
                                 'dim_datetimes'     : './data/modeled/dim_datetimes',
                                 'dim_meters'        : './data/modeled/dim_meters',
                                 'dim_bills'         : './data/modeled/dim_bills',
                                 'fct_electric_brew' : './data/modeled/fct_electric_brew'}) -> dd.DuckDBPyConnection:
    '''
    This function creates views in a DuckDB database for the Electric Brew project by reading data from 
    parquet files. It leverages DuckDB's ability to directly query Parquet files, which simplifies the 
    data loading process compared to a traditional SQL database approach.

    These views will automatically change as the underlying Parquet data changes.

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

    db = dd.connect(find_project_root(path))

    for k, v in vws.items():
        try:
            # Create SQL view for each parquet file
            db.execute(f"DROP VIEW IF EXISTS {k}")
            db.execute(f"CREATE VIEW {k} AS SELECT * FROM read_parquet('{find_project_root(v)}/**/*.parquet')")

        except Exception as e:
            lg.error(f"Error occurred while creating view '{k}': {e}\n")
            raise e

    return db