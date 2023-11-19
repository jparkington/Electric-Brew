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




from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def write_results(data: pd.DataFrame, 
                       destination_path: str, 
                       file_type: str = 'Parquet', 
                       add_id: bool = True, 
                       partition_col: Optional[str] = None):
    """
    Write processed data to a file in the specified format.

    Args:
        data (pd.DataFrame): The DataFrame to be written.
        destination_path (str): Path to the destination directory.
        file_type (str, optional): The type of file to write (CSV, Parquet). Defaults to 'Parquet'.
        add_id (bool, optional): Whether to add a unique identifier to the data. Defaults to True.
        partition_col (str, optional): Column to partition by. Defaults to None.
    """
    if add_id:
        data['id'] = range(1, len(data) + 1)

    if file_type.upper() == 'PARQUET':
        if partition_col:
            pq.write_to_dataset(pa.Table.from_pandas(data), root_path=destination_path, partition_cols=[partition_col])
        else:
            data.to_parquet(os.path.join(destination_path, 'output.parquet'), index=False)
    elif file_type.upper() == 'CSV':
        data.to_csv(os.path.join(destination_path, 'output.csv'), index=False)
    # Add more conditions here for different file types
