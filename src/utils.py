from cycler            import cycler
from matplotlib.pyplot import rcParams
from seaborn           import color_palette

import os
import logging         as lg
import pandas          as pd
import pyarrow         as pa
import pyarrow.parquet as pq

# Set up a logging configuration
lg.basicConfig(level  = lg.INFO, 
               format = '%(asctime)s | %(levelname)s | %(message)s')

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
