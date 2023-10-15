<!-- omit in toc -->
# Directory Documentation for `/src/`

The `/src/` directory houses various utility scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. Our instance of `/src/` houses scripts responsible for data curation, output formatting, and visualization. This document provides a brief overview of each script and its contained functions.

<!-- omit in toc -->
## Table of Contents

- [`utils.py`](#utilspy)
  - [Runtime](#runtime)
    - [`set_plot_params`](#set_plot_params)
    - [`read_data`](#read_data)
  - [DataFrames](#dataframes)
  - [Curation](#curation)
    - [`curate_meter_usage`](#curate_meter_usage)
    - [`scrape_bills`](#scrape_bills)

## `utils.py`

The `utils.py` script provides a variety of utility functions spanning different aspects of the project, from visual parameter configuration to comprehensive data curation tasks.

### Runtime

This section contains functions primarily focused on setting up and configuring the environment for data visualization and data reading. These functions make sure that all plots have a uniform appearance and that data files can be easily read into Pandas DataFrames.

#### `set_plot_params`

**Purpose**  
Initializes and returns custom plotting parameters for `matplotlib`, ensuring consistent visual style throughout the project.

**Signature** 
```python
def set_plot_params() -> list:
```

**Returns**  
A list containing RGBA color tuples that comprise the custom color palette for plots.

#### `read_data`

**Purpose**  
Reads `.parquet` files into Pandas DataFrames. Parquet is a columnar storage file format optimized for use with big data frameworks. This utility makes it simple to ingest such data for analysis or transformation.

**Signature** 
```python
def read_data(file_path : str) -> pd.DataFrame:
```

**Returns**  
A DataFrame containing the data read from the supplied Parquet file path.

### DataFrames

This section initializes commonly used DataFrames at the start, making them readily available across different parts of the project. This promotes code reusability and performance optimization.

### Curation

This section comprises functions that transform raw data files into structured and query-optimized formats. This includes converting raw CSVs into partitioned Parquet files and extracting relevant data from PDFs.

#### `curate_meter_usage`

**Purpose** Processes all CSV files from the provided directory, integrates the specified schema, and subsequently consolidates the data into a partitioned `.parquet` file in the designated output directory. This conversion and curation process is optimized with `snappy` compression for efficiency.

**Signature** 
```python
def curate_meter_usage(raw           : str, 
                       curated       : str, 
                       partition_col : list, 
                       schema        : list):
```

**Parameters**

- **`raw`**: Path to the directory containing raw `meter-usage` CSV files.

- **`curated`** : Directory where the consolidated `.parquet` files will be saved.

- **`partition_col`**: Columns by which the .parquet files will be partitioned.

- **`schema`**: Columns that will be used as headers in the resulting DataFrame.

#### `scrape_bills`

**Purpose**  
This function automates the extraction of specific fields from a collection of PDF bills stored in a directory. It is designed to capture around 90% of the values from these bills. However, due to variations in the format and content of individual documents, manual review and intervention is required for complete accuracy.

**Signature** 
```python
def scrape_bills(raw    : str, 
                 output : str):
```

**Parameters**

- **`raw`**: Path to the directory containing the PDF bills that need to be processed.

- **`output`** : Path where the extracted data will be saved as a CSV file.

**Regular Expressions**

The `scrape_bills` function uses various regular expressions (REGEX) to identify and extract specific pieces of information from text content within utility bills stored as PDF files. Below, each REGEX is broken down to explain its components and what it aims to capture:

1. **Account Number**: `r"Account Number\s*([\d-]+)"`
    - `Account Number`: Literal text that the REGEX searches for.
    - `\s*`: Matches zero or more whitespace characters.
    - `([\d-]+)`: Captures one or more digits or dashes.
    
    **Purpose**  
    Captures the account number that follows the literal text "Account Number", allowing for possible whitespace and dashes.

2. **Amount Due**: `r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"`
    - `Amount Due Date Due`: Literal text that the REGEX searches for.
    - `\s*`: Matches zero or more whitespace characters.
    - `\d+-\d+-\d+`: Captures a date in the format `d+-d+-d+`.
    - `[A-Z\s]+`: Captures one or more uppercase letters or whitespace.
    - `\$\s*`: Captures the dollar sign and any following whitespace.
    - `([\d,]+\.\d{2})`: Captures one or more digits or commas, followed by a decimal and exactly two digits.

    **Purpose**  
    Finds the amount due that follows the literal text "Amount Due Date Due", capturing the date and any uppercase letters or whitespace, accounting for optional whitespace, and ensures the amount has two decimal places.

3. **Service Charge**: `r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})"`
    - `Service Charge`: Literal text that the REGEX starts with.
    - `.*?`: Lazily matches any number of any characters.
    - `@\$\s*`: Captures the '@$' symbol followed by any number of whitespace characters.
    - `([+-]?\d+\.\d{2})`: Captures a number that may be positive or negative, followed by a decimal and exactly two digits.
    
    **Purpose**  
    Captures the service charge value found after the term "Service Charge", accounting for both positive and negative values and ensuring two decimal places.

4. **Delivery Service Rate**: `r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"`
    - `Delivery Service`: Literal text to start the REGEX.
    - `[:\s]*`: Captures a colon or any number of whitespace characters.
    - `\d+,?\d+ KWH`: Captures one or more digits, optionally a comma, and more digits followed by ' KWH'.
    - `@\$(\d+\.\d+)`: Captures the '@$' symbol followed by one or more digits, a decimal point, and more digits.
    
    **Purpose**  
    Extracts the rate per kilowatt-hour (KWH) for delivery service, following the term "Delivery Service".

5. **Meter Details**: `r"Delivery Charges.*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,4},?\d{0,3}) KWH.*?Total Current Delivery Charges"`
    - `Delivery Charges.*?`: Starts with the literal text and lazily matches any number of any characters.
    - `(\d{1,2}/\d{1,2}/\d{4})`: Captures a date in the format MM/DD/YYYY.
    - `.*?`: Lazily matches any number of any characters.
    - `(\d{1,4},?\d{0,3}) KWH`: Captures one or more digits, optionally a comma, and more digits followed by ' KWH'.
    - `.*?Total Current Delivery Charges`: Lazily matches any number of any characters until it finds the text 'Total Current Delivery Charges'.

    **Purpose**  
    Captures multiple details (read date, prior read date, and kilowatt-hours (KWH) delivered) within the section starting with "Delivery Charges".