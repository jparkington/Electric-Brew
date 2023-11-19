from utils import *

'''
write_result(): Needs better name; accept a type argument (CSV, Parquet [Default]); add_id argument (True [Default]); partition_col (None [Default]) that works with both Parquet and a CSV structure if applicable

While at it, the `scrape_cmp_bills` logic should be reworked to land the curated CSVs in the same folder structure as the PDFs
For glob, is there a simpler way to say "look at all subdirectories after this path, even if there are none, to find this file extension"?

The remaining "logic" should just be the regex strings for each scraper
'''

# Example usage
# load_data_files("path/to/csv_files", "CSV", ["col1", "col2"])
# load_data_files("path/to/pdf_files", "PDF")

# Example usage
# write_results(data_df, "path/to/destination", "CSV")
# write_results(data_df, "path/to/destination", "Parquet", partition_col="some_column")



import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging as lg

def write_results(data           : pd.DataFrame, 
                  destination    : str, 
                  add_id         : bool = True, 
                  partition_by   : str  = None,
                  compression    : str  = 'snappy',
                  use_dictionary : bool = True):
    """
    Write processed data to a Parquet file.

    Methodology:
        1. Optionally add a unique identifier to the data.
        2. Write the DataFrame to a Parquet file, handling partitioning if required.

    Parameters:
        data           (pd.DataFrame) : The DataFrame to be written.
        destination    (str)          : Path to the destination directory.
        add_id         (bool)         : Whether to add a unique identifier to the data. Defaults to True.
        partition_by   (str)          : Column to partition by. Defaults to None.
        compression    (str)          : Compression method for Parquet files. Defaults to 'snappy'.
        use_dictionary (bool)         : Whether to enable dictionary encoding. Defaults to True.
    """
    
    if add_id:
        data['id'] = range(1, len(data) + 1)

    try:
        pq.write_to_dataset(pa.Table.from_pandas(data), 
                            root_path      = destination, 
                            partition_cols = [partition_by] if partition_by else None,
                            compression    = compression,
                            use_dictionary = use_dictionary)

        lg.info(f"Data written in Parquet to {destination}.")

    except Exception as e:
        lg.error(f"Error writing data to {destination}: {e}")
