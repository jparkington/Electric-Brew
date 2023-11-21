from datetime          import datetime
from glob              import glob
from re                import findall, search
from shutil            import rmtree
from typing            import *
from variables         import locations

import os
import logging         as lg
import pandas          as pd
import pdfplumber      as pl
import pyarrow         as pa
import pyarrow.parquet as pq

lg.basicConfig(level  = lg.INFO, 
               format = '%(asctime)s | %(levelname)s | %(message)s')

'''
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
