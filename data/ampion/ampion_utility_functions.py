'''
    This contains all the utility functions that will read in and process the Ampion bill pdfs
'''
import pdfplumber as pl
import re
import pandas as pd
import os
import pyarrow         as pa
import pyarrow.parquet as pq
import logging         as lg


##################################################################################
################################### Open PDF #####################################
##################################################################################
def open_pdf(path) -> str:
    ''' 
        Function: open_pdf
        Pamameters: 1 string
            path: the path to Ampion pdf bills files
        Returns: 1 string
            text: the extracted text from the pdf

        This file function will open one page pdf and extract the text
    '''
    try: 
        pdf = pl.open(path)
        page = pdf.pages[0]
        text = page.extract_text()

    except FileNotFoundError as e:
        print(f"File not found: {path}")
        return None
    except pl.pdf.PDFSyntaxError as e:
        print(f"Invalid PDF file: {path}")
        return None
    except Exception as e:
        print(f"Error processing PDF at {path}: {e}")
        return None
    finally:
        if pdf is not None:
            pdf.close()
    return text

######################################################################################
################################### Find Matches #####################################
######################################################################################
def find_matches(expression, search_space)-> list:
    ''' 
        Function: find_matches
        Parameters: 2 strings
            expression: the regex search query
            search_space: the text to search
        Returns: 1 list
            matches: a list of strings or tuples that match the query

        This function execute a regex search
    '''
    try:

        matches = [match for match in re.findall(expression, search_space)]
        return matches

    except re.error as e:
        print(f"Regex error: {e}")
        return None
    
######################################################################################
############################## Execute Regex Search ##################################
######################################################################################
def execute_regex_search(searches, search_space)-> pd.DataFrame:
    ''' 
        Function: execute_regex_search
        Parameters: 1 list, 1 string
            searches: a list of regex searches
            search_space: the text to be searched
        Returns: 1 DataFrame

        This function will call find_matches and put the results from a number of searches in 
        a pandas dataframe
    '''
    matched_data = [find_matches(expression, search_space) for expression in searches]

    df = pd.DataFrame(matched_data).T

    df.columns = ['abbr_account_number', 'date_range', 'kwh_and_bill_credits', 'price']

    return df

######################################################################################
############################### Clean Regex Dataframe ################################
######################################################################################
def clean_regex_df(df) -> pd.DataFrame:
    '''  
        Function: clean_regex_df
        Parameters: 1 pd.DataFrame
            df: the df produced by execute_regex_search
        Returns: 1 pd.DataFrame
            df: a cleaned version of the df produced by execute_regex_search

        This function will take the raw results from the regex search df and make put them in a
        usable form
    '''
    # this will separate the start and end dates
    pattern = r"(\d{2}\.\d{2}\.\d{4}) – (\d{2}\.\d{2}\.\d{4})"
    df[['start_date', 'end_date']] = df['date_range'].str.extract(pattern)

    # this will convert the columns to datetime objects
    df['start_date'] = pd.to_datetime(df['start_date'], format='%m.%d.%Y')
    df['end_date'] = pd.to_datetime(df['end_date'], format='%m.%d.%Y')

    # this will separate the description column
    df[['production?', 'bill_credits_allocated']] = df['kwh_and_bill_credits'].apply(pd.Series)

    # this will separate the price column
    df[['bill_credit_received', 'standard_price', 'reduced_price']] = df['price'].apply(pd.Series)
    
    # this will drop the original columns 
    df.drop(['kwh_and_bill_credits', 'date_range', 'price'], axis = 1, inplace = True)

    # this section is hard coded, 
    # this invoice is the only one that uses 'standard_price'
    if df.invoice_number[0] == 2022120000512891:
        df.rename(columns = {'standard_price': 'price'}, inplace = True)
        df.drop('reduced_price', axis = 1, inplace = True)
    else:
        df.rename(columns = {'reduced_price': 'price'}, inplace = True)
        df.drop('standard_price', axis = 1, inplace = True)

    # one the bills had a credit disbursement, unclear where to file that
    if len(df) > 7:
        df = df.iloc[:7]

    return df

######################################################################################
################################### Process PDF ######################################
######################################################################################
def process_pdf(text):
    ''' 
        Function: process_pdf
        Parameters: 1 string
            text: the extracted text from the pdf
        Returns: 1 pd.DataFrame
            df: the cleaned df of all the Ampion bills
        
        This function will invoke all the helper functions. This is where the specific regex searches are
        defined. It will return a cleaned dataframe with the Ampion bill data.
    '''
    # this will grab the invoice number
    invoice_number_pattern = r"Invoice:\s(\d+)"
    invoice_number = re.search(invoice_number_pattern, text).group(1)
    invoice_number = int(invoice_number)

    # first define the searches you want executed
    account_number = r'\*{5}(\d+)'
    description = r'(\d+(?:,\d{3})*\s*kWh)\s+and\s+\$(\d+(?:,\d{3})*\.\d{2})'
    prices = r'allocated\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})'
    date = r'(\d{2}\.\d{2}\.\d{4}\s*–\s*\d{2}\.\d{2}\.\d{4})'

    # put the searches in a list
    searches = [account_number, date, description, prices]

    # call execute_regex_search, this will use the helper function find_matches
    df = execute_regex_search(searches, text)

    # this will add the invoice number to an invoice number column
    df['invoice_number'] = invoice_number

    df = clean_regex_df(df)

    return df

######################################################################################
#################################### Get Data ########################################
######################################################################################
def get_data() -> pd.DataFrame and list:
    ''' 
        Function: get_data
        Parameters: None
        Returns: 1 pd.DataFrame, 1 list

        This function will cycle through the files in the 'Ampion/raw/bills/' folder.
        It will open the pdfs and scrape the data using the helper functions. It will return one
        cleaned dataframe and list of the unique account numbers.
    '''
    # this is the path to the folder
    folder_path = 'data/ampion/raw/bills'

    # list to store results
    all_results = []

    try:
        # iterate over the files
        for filename in os.listdir(folder_path):
            if filename.endswith('.pdf'):

                # get the file path
                file_path = os.path.join(folder_path, filename)

                # open the pdf using helper functions
                text = open_pdf(file_path)

                # process the pdf using helper functions
                df = process_pdf(text)

                # append results to the all_results list
                all_results.append(df)

    except FileNotFoundError as file_not_found_error:
        print(f"File not found error for {file_path}: {file_not_found_error}")
        raise FileNotFoundError(f"File not found error for {file_path}")
    except Exception as other_error:
        print(f"Other error processing PDF {file_path}: {other_error}")
        raise Exception(f"Other error processing PDF {file_path}")

    try: 
        # concatenate all the DataFrames into a single DataFrame
        df = pd.concat(all_results, ignore_index = True)

        # create list of account_numbers
        account_numbers = df.abbr_account_number.unique()

        return df, account_numbers

    except Exception as concat_error:
        print(f"Error concatenating DataFrames: {concat_error}")
        return None, None
    
######################################################################################
######################### Export CSV and Parquet Files ###############################
######################################################################################
def export_csv_and_parquet_files(df: pd.DataFrame, account_numbers: list) -> None:
    '''
        Function: export_csv_and_parquet_files
        Parameters: None
        Returns: None

        This function will take the cleaned dataframe generated from the helper functions
        and filter by account_number, exporting a csv and parquet file for each account
    '''
    try:
        folder_path = 'data/ampion/curated/csv_files'
        for i in account_numbers:
            account_df = df.loc[df['abbr_account_number'] == i]
            file_path = str(i)
            file_path = os.path.join(folder_path, file_path + '.csv') 
            account_df.to_csv(file_path, index=False)
            
    except Exception as csv_error:
            print(f"Error exporting CSV for account {i}: {csv_error}")

'''   
    try:
        folder_path = 'data/ampion/curated/parquet_files'
        for i in account_numbers:
            account_df = df.loc[df['abbr_account_number'] == i]
            file_path = str(i)
            file_path = os.path.join(folder_path, file_path + '.parquet')  
            account_df.to_parquet(file_path, index=False)
    except Exception as parquet_error:
            print(f"Error exporting Parquet for account {i}: {parquet_error}")
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
