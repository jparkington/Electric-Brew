<!-- omit in toc -->
# Directory Documentation for `/src/`

The `/src/` directory contains various utility scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. Our instance of `/src/` houses scripts responsible for data curation, output formatting, and visualization. This document provides a brief overview of each script and its contained functions.

> **Note**: To facilitate smooth development and execution, it's recommended to run all commands out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** is set to point directly to the `src` directory within this Conda environment. This allows you to easily import the `utils` module and its DataFrames and functions from any script within the `src` directory.

<!-- omit in toc -->
## Table of Contents

- [`utils.py`](#utilspy)
  - [Runtime](#runtime)
    - [`set_plot_params`](#set_plot_params)
    - [`read_data`](#read_data)
  - [DataFrames](#dataframes)
    - [`meter_usage`](#meter_usage)
  - [Curation](#curation)
    - [`curate_meter_usage`](#curate_meter_usage)
    - [`scrape_cmp_bills`](#scrape_cmp_bills)
      - [**Regular Expressions**](#regular-expressions)
      - [**Manual Interventions**](#manual-interventions)

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
Reads `.parquet` files into Pandas DataFrames. The function resolves the path relative to the `data` directory of the project, no matter where your script is located within the `src` directory or where you've cloned the repo. As such, it expects a file path string that starts from within the `data` directory.

**Signature** 
```python
def read_data(file_path : str) -> pd.DataFrame:
```

**Returns**  
A DataFrame containing the data read from the supplied Parquet file path.

### DataFrames

This section initializes commonly used DataFrames at the start, making them readily available across different parts of the project. This promotes code reusability and performance optimization.

#### `meter_usage`

A repository for meter-level electrical consumption data from Central Maine Power (CMP) in 15-minute intervals. Used in analyses of electricity usage patterns, billing, and location-related insights. The DataFrame is partitioned by `account_number`, enabling quick data retrieval for individual accounts. 

**Source**: Central Maine Power (CMP)  
**Location**: `..data/cmp/curated/meter-usage`  
**Partitioning**: `account_number`  

**Schema**:

  - `service_point_id` (**int**): A unique identifier for the point where the electrical service is provided, often tied to a specific location or customer.
  
  - `meter_id` (**str**): Identifier for the electrical meter installed at the service point. It records the amount of electricity consumed.
  
  - `interval_end_datetime` (**str**): Timestamp marking the end of the meter reading interval, typically indicating when the meter was read.
  
  - `meter_channel` (**int**): The channel number on the electrical meter. Meters with multiple channels can record different types of data.
  
  - `kwh` (**float**): Kilowatt-hours recorded by the meter during the interval, representing the unit of electricity consumed.

  - `account_number` (**int**): A unique identifier for the customer's account with CMP.

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

#### `scrape_cmp_bills`

**Purpose**  
This function automates the extraction of specific fields from a collection of PDF bills stored in a directory. It is designed to capture around 90% of the values from these bills. However, due to variations in the format and content of individual documents, manual review and intervention is required for complete accuracy.

**Signature** 
```python
def scrape_cmp_bills(raw    : str, 
                     output : str):
```

**Parameters**

- **`raw`**: Path to the directory containing the PDF bills that need to be processed.

- **`output`** : Path where the extracted data will be saved as a CSV file.

##### **Regular Expressions**

The `scrape_cmp_bills` function uses various regular expressions (RegEx) to identify and extract specific pieces of information from text content within utility bills stored as PDF files. Below, each RegEx is broken down to explain its components and what it aims to capture. Patterns were detected and tested using [**RegExr**](https://regexr.com), a visual IDE for finding RegEx patterns within blocks of text.

1. **Account Number**: `r"Account Number\s*([\d-]+)"`
    - `Account Number`: Literal text that the RegEx searches for.
    - `\s*`: Matches zero or more whitespace characters.
    - `([\d-]+)`: Captures one or more digits or dashes.
    
    **Purpose**  
    Captures the account number that follows the literal text "Account Number", allowing for possible whitespace and dashes.

2. **Amount Due**: `r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"`
    - `Amount Due Date Due`: Literal text that the RegEx searches for.
    - `\s*`: Matches zero or more whitespace characters.
    - `\d+-\d+-\d+`: Captures a date in the format `d+-d+-d+`.
    - `[A-Z\s]+`: Captures one or more uppercase letters or whitespace.
    - `\$\s*`: Captures the dollar sign and any following whitespace.
    - `([\d,]+\.\d{2})`: Captures one or more digits or commas, followed by a decimal and exactly two digits.

    **Purpose**  
    Finds the amount due that follows the literal text "Amount Due Date Due", capturing the date and any uppercase letters or whitespace, accounting for optional whitespace, and ensures the amount has two decimal places.

3. **Service Charge**: `r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})"`
    - `Service Charge`: Literal text that the RegEx starts with.
    - `.*?`: Lazily matches any number of any characters.
    - `@\$\s*`: Captures the '@$' symbol followed by any number of whitespace characters.
    - `([+-]?\d+\.\d{2})`: Captures a number that may be positive or negative, followed by a decimal and exactly two digits.
    
    **Purpose**  
    Captures the service charge value found after the term "Service Charge", accounting for both positive and negative values and ensuring two decimal places.

4. **Delivery Service Rate**: `r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"`
    - `Delivery Service`: Literal text to start the RegEx.
    - `[:\s]*`: Captures a colon or any number of whitespace characters.
    - `\d+,?\d+ KWH`: Captures one or more digits, optionally a comma, and more digits followed by ' KWH'.
    - `@\$(\d+\.\d+)`: Captures the '@$' symbol followed by one or more digits, a decimal point, and more digits.
    
    **Purpose**  
    Extracts the rate per kilowatt-hour (kWh) for delivery service, following the term "Delivery Service".

5. **Meter Details**: `r"Delivery Charges.*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,4},?\d{0,3}) KWH.*?Total Current Delivery Charges"`
    - `Delivery Charges.*?`: Starts with the literal text and lazily matches any number of any characters.
    - `(\d{1,2}/\d{1,2}/\d{4})`: Captures a date in the format MM/DD/YYYY.
    - `.*?`: Lazily matches any number of any characters.
    - `(\d{1,4},?\d{0,3}) KWH`: Captures one or more digits, optionally a comma, and more digits followed by ' KWH'.
    - `.*?Total Current Delivery Charges`: Lazily matches any number of any characters until it finds the text 'Total Current Delivery Charges'.

    **Purpose**  
    Captures multiple details (interval start, interval end, and kilowatt-hours (kWh) delivered) within the section starting with "Delivery Charges".

##### **Manual Interventions**

After the automated scraping process is completed by `scrape_cmp_bills`, several manual inputs are generally required to ensure data accuracy and completeness. Here are the steps for those interventions:

1. **File Relocation and Renaming**:  
    - **Initial State**: A CSV file named `scraped_bills.csv` is generated in the `cmp/raw/bills` directory.  
    - **Action**: Rename this file to `scraped_bills_with_edits` and move it to `cmp/curated/bills` to avoid accidental overwrites of manually curated data.

2. **Handling Null 'Amount Due'**:  
    - **Scenario**: Sometimes the `amount_due` field is NULL if the bill is fully paid off.  
    - **Action**: Manually enter a 0 for such cases.  
    - **Example**: See [`30010320353/700000447768_bill.pdf`](../data/cmp/raw/bills/30010320353/700000447768_bill.pdf).

3. **Managing Multiple Billing Intervals**:  
    - **Scenario**: Some bills may have more than one billing interval, often crossing fiscal quarters.  
    - **Action**: Add an additional row in the CSV with the same `account_number`, `amount_due`, and `pdf_file_name`, but with differing values for other fields.  
        - **Special Case**: If the bill presents a single `kwh_delivered` value for multiple intervals, allocate a percentage of this to each interval based on the percentage of total `service_charge` paid.  
    - **Example**: See [`30010320353/703001515406_bill.pdf`](../data/cmp/raw/bills/30010320353/703001515406_bill.pdf) and [`30010320353/702001847715_bill.pdf`](../data/cmp/raw/bills/30010320353/702001847715_bill.pdf).

4. **Addressing Absent 'Delivery Service Rate'**:  
    - **Scenario**: Some bills do not include a `delivery_service_rate`.  
    - **Action**: Leave this field as NULL, with the understanding that the delivery was covered by previous `Banked Generation`.  
    - **Example**: See [`30010320353/705001871139_bill.pdf`](../data/cmp/raw/bills/30010320353/705001871139_bill.pdf).

5. **External Electricity Supply**:  
    - **Scenario**: In cases where electricity is supplied by an external provider.  
    - **Action**: Add an additional row to the CSV to capture the external supplier's incremental rate for the number of kWh delivered.  
    - **Example**: See [`30010320353/701001868909_bill.pdf`](../data/cmp/raw/bills/30010320353/701001868909_bill.pdf).

After these manual interventions are performed, the curated CSV file is ready for conversion into Parquet format for further data processing.
