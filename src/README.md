<!-- omit in toc -->
# Utility Functions

The `/src/` directory contains a `utils` directory full of scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. This document provides a brief overview of each function and its purpose, signature, and expected usage.

> **Note**: To facilitate smooth development and execution, it's recommended to run all commands out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** is set to point directly to the `/src/` directory within this Conda environment. This allows you to easily import any function or variable within the `utils` module into any script within `/src/`.

<!-- omit in toc -->
## Table of Contents
- [`curation.py`](#curationpy)
  - [`load_data_files`](#load_data_files)
  - [`write_results`](#write_results)
  - [`scrape_cmp_bills`](#scrape_cmp_bills)
    - [**Regular Expressions**](#regular-expressions)
  - [`scrape_ampion_bills`](#scrape_ampion_bills)
    - [**Regular Expressions**](#regular-expressions-1)
- [`etl.py`](#etlpy)
  - [Overview](#overview)
  - [Script Execution Flow](#script-execution-flow)
  - [Detailed Function Descriptions](#detailed-function-descriptions)
- [`modeling.py`](#modelingpy)
  - [`model_dim_datetimes`](#model_dim_datetimes)
  - [`model_dim_meters`](#model_dim_meters)
  - [`model_dim_bills`](#model_dim_bills)
  - [`model_fct_electric_brew`](#model_fct_electric_brew)
- [`runtime.py`](#runtimepy)
  - [`set_plot_params`](#set_plot_params)
  - [`read_data`](#read_data)
  - [`connect_to_db`](#connect_to_db)
- [`variables.py`](#variablespy)
  - [Overview](#overview-1)
  - [DataFrames and Database Initialization](#dataframes-and-database-initialization)
    - [Curated DataFrames](#curated-dataframes)
    - [Modeled DataFrames](#modeled-dataframes)
    - [DuckDB Database Connection](#duckdb-database-connection)
  - [Data Dictionary](#data-dictionary)


## [`curation.py`](utils/curation.py)

This section comprises functions that transform raw data files into structured and query-optimized formats. This includes converting raw CSVs into partitioned Parquet files and extracting relevant data from PDFs.

### `load_data_files`

This function efficiently loads data files from a specified directory, accommodating multiple file types, including CSV, PDF, and Parquet. It is designed to streamline the data integration process, allowing for the consolidation of various data sources into a single, manageable format.

**Signature**
```python
def load_data_files(path : str, 
                    type : str       = 'CSV', 
                    cols : List[str] = None) -> Union[pd.DataFrame, str]
```

**Parameters**
- **`path`**: The directory path where the data files are stored.

- **`type`**: Specifies the format of the files to be processed. This function supports 'CSV' for comma-separated values, 'PDF', and 'Parquet' for the columnar storage format. Default is set to 'CSV'.

- **`cols`**: An optional list of column names to apply as headers when loading CSV files, allowing for the customization of data structure right at the loading stage.

**Functionality**
1. **File Type Determination**: The function first identifies the type of files to be processed, preparing the necessary procedures for each file format.
2. **CSV Files Handling**: For CSV files, the function loads each file individually, applies the specified column names if provided, and then concatenates all data into a single DataFrame.
3. **PDF Files Processing**: For PDF files, the function extracts text content from each page and compiles this information into a DataFrame, with each row representing a page.
4. **Parquet Files Loading**: For Parquet files, the function directly reads the dataset from the given directory, leveraging Parquet's efficient columnar storage format and partitioning.

### `write_results`

This function is writes processed data into Parquet files, stored in a designated directory. It offers flexibility in managing data output, including options for adding a primary key, partitioning data based on specified columns, applying `snappy` compression for efficient storage, and choosing between overwriting or appending to existing data. This function is crucial for the final stage of data curation, ensuring data is stored in an optimized and organized manner for future retrieval and analysis.

**Signature**
```python
def write_results(data           : pd.DataFrame, 
                  dest           : str, 
                  add_id         : bool = False, 
                  partition_by   : str  = 'account_number', 
                  compression    : str  = 'snappy', 
                  use_dictionary : bool = True, 
                  overwrite      : bool = True)
```

**Parameters**
- **`data`**: The DataFrame to be exported into Parquet format.

- **`dest`**: The file path for the destination directory.

- **`add_id`**: A flag indicating whether to add a sequential identifier column ('id') to the DataFrame, which acts as a primary key.

- **`partition_by`**: An optional parameter to specify a column by which the data should be partitioned, enhancing data organization and retrieval efficiency.

- **`compression`**: Determines the compression method for the Parquet files. 'Snappy' is selected by default for its balance of compression integrity and performance.

- **`use_dictionary`**: A flag to enable or disable dictionary encoding within the Parquet files, which can improve performance and compression for datasets with repeated values.

- **`overwrite`**: Determines whether to overwrite existing data in the destination directory or append to it.

**Functionality**
1. **Directory Preparation**: Checks if the specified destination directory exists and prepares it for data writing, creating it if necessary or handling overwriting and appending based on the `overwrite` flag.
2. **ID Column Addition**: If `add_id` is True, the function adds a unique identifier column to the DataFrame, enhancing data traceability.
3. **Data Writing**: Executes the process of converting the DataFrame into Parquet format and writing it to the specified destination, taking into account partitioning and compression settings.

### `scrape_cmp_bills`
 
This function reads all PDFs in a specified directory, extracting specific information from CMP bills using regular expressions. Each bill is dissected into structured records that represent single delivery groups, with the aim of capturing complete data across all pages of the bill.

**Signature** 
```python
def scrape_cmp_bills(raw    : str = "./data/cmp/raw/bills/pdf",
                     output : str = "./data/cmp/raw/bills/parquet"):
```

**Parameters**

- **`raw`**: Path to the directory containing the PDF bills that need to be processed.

- **`output`** : Path where the extracted data will be saved as a Parquet directory.

#### **Regular Expressions**

The `scrape_cmp_bills` function employs a series of regular expressions designed to target specific data points within the bills. Some patterns are intended for early pages (like invoice and account numbers), while others are for later pages (such as delivery charges and supplier information). This approach ensures that all relevant billing information, regardless of its location within the document, is accurately captured and recorded.

1. **Amount Due**: `r"Amount Due.*?\$\s*(\d+\.\d{2})"`
     - `Amount Due`: Literal text that the RegEx searches for.
     - `.*?`: Lazily matches any number of any characters.
     - `\$\s*`: Matches the dollar sign followed by any whitespace.
     - `(\d+\.\d{2})`: Captures a monetary value with two decimal places.

2. **Delivery Tax**: `r"Maine Sales Tax \+\$(\d+\.\d{2})"`
     - `Maine Sales Tax`: Literal text indicating the start of the tax information.
     - `\+\$`: Matches the plus sign and dollar sign.
     - `(\d+\.\d{2})`: Captures a monetary value with two decimal places.

3. **Delivery Group**: `r"Delivery Charges:.*?\(\s*(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})\s*\)"`
     - `Delivery Charges:`: Literal text indicating the start of the delivery charges section.
     - `.*?`: Lazily matches any number of any characters until the date range.
     - `\(\s*`: Matches the opening parenthesis and any whitespace.
     - `(\d{2}/\d{2}/\d{4})`: Captures a date in the format MM/DD/YYYY.
     - `\s*-\s*`: Matches the dash between dates with any surrounding whitespace.
     - `(\d{2}/\d{2}/\d{4})`: Captures a second date in the format MM/DD/YYYY.
     - `\s*\)`: Matches the closing parenthesis and any whitespace.

4. **Service Charge**: `r"Service Charge.*?\+\$(\d+\.\d{2})"`
     - `Service Charge`: Literal text indicating the start of the service charge.
     - `.*?`: Lazily matches any number of any characters until the dollar amount.
     - `\+\$`: Matches the plus sign and dollar sign.
     - `(\d+\.\d{2})`: Captures a monetary value with two decimal places.

5. **Delivery Service**: `r"Delivery Service: ([\d,]+) KWH (?:@\$\d+\.\d{6} )?\+\$(\d+\.\d{2})"`
     - `Delivery Service:`: Literal text indicating the start of the delivery service section.
     - `([\d,]+) KWH`: Captures the kWh amount, which may include commas.
     - `(?:@\$\d+\.\d{6} )?`: Optionally matches a rate per kWh, followed by whitespace.
     - `\+\$`: Matches the plus sign and dollar sign.
     - `(\d+\.\d{2})`: Captures a monetary value with two decimal places.

6. **Supplier Information**: `r"Prior Balance for ([A-Z\s\w.]+)(?: Supplier)? \$\d+\.\d{2}"`
     - `Prior Balance for`: Literal text indicating the start of the supplier information section.
     - `([A-Z\s\w.]+)`: Captures the supplier's name, allowing for uppercase letters, spaces, word characters, and periods.
     - `(?: Supplier)?`: Optionally matches the word "Supplier".
     - `\$\d+\.\d{2}`: Matches a monetary value (not captured) with two decimal places.

7. **kWh Supplied**: `r"Energy Charge ([\d,]+) KWH"`
     - `Energy Charge`: Literal text indicating the start of the energy charge section.
     - `([\d,]+) KWH`: Captures the kWh amount, which may include commas.

8. **Supply Charge**: `r"Energy Charge.*?\+\$(\d+\.\d{2})"`
     - `Energy Charge`: Literal text indicating the start of the supply charge.
     - `.*?`: Lazily matches any number of any characters until the dollar amount.
     - `\+\$`: Matches the plus sign and dollar sign.
     - `(\d+\.\d{2})`: Captures a monetary value with two decimal places.

9. **Supply Tax**: `r"Maine Sales Tax \+\$(\d+\.\d{2})"`
     - `Maine Sales Tax`: Literal text indicating the start of the supply tax.
     - `\+\$`: Matches the plus sign and dollar sign.
     - `(\d+\.\d{2})`: Captures a monetary value with two decimal places.


### `scrape_ampion_bills`
 
This function automates the extraction of specific fields from a collection of PDF bills stored in a directory. In this instance, the design of the regular expression captures all patterns successfully. In addition, there are some conditional measures to respond to the location of visible elements like the "Your Price" banner. A manual intervention is performed for "Miscellaneous Charges" that do not fit the structure of the bills.

**Signature** 
```python
def scrape_ampion_bills(raw    : str = "./data/ampion/raw/pdf", 
                        output : str = "./data/ampion/raw/parquet"):
```

**Parameters**

- **`raw`**: Path to the directory containing the raw PDF utility bills that need to be processed.

- **`output`**: Path where the extracted data will be saved as a Parquet directory.

#### **Regular Expressions**

The `scrape_ampion_bills` function uses various regular expressions to accurately extract specific information from Ampion PDF utility bills. This includes standard charges and conditional "Miscellaneous Charges." The `r_misc` patterns are only used if "Miscellaneous Charges" are present in the content.

1. **Invoice Number**: `r"Invoice:\s(\d+)"`
   - `Invoice:`: Literal text the RegEx searches for.
   - `\s`: Matches a single whitespace character.
   - `(\d+)`: Captures one or more digits.

2. **Abbreviated Account Number**: `r'\*{5}(\d+)'`
   - `\*{5}`: Matches exactly five asterisk characters.
   - `(\d+)`: Captures one or more digits.

3. **Date Ranges**: `r'(\d{2}\.\d{2}\.\d{4})\s*–\s*(\d{2}\.\d{2}\.\d{4})'`
   - `(\d{2}\.\d{2}\.\d{4})`: Captures a date in the format DD.MM.YYYY.
   - `\s*–\s*`: Matches a dash surrounded by optional whitespace.
   - Another `(\d{2}\.\d{2}\.\d{4})`: Captures a second date in the same format.

4. **Kilowatt-Hour (kWh) Values**: `r'(\d{1,4}(?:,\d{3})*?) kWh'`
   - `(\d{1,4}(?:,\d{3})*?)`: Captures a numeric value, allowing for comma-separated thousands.
   - `kWh`: Literal text indicating the unit of energy.

5. **Prices**: `r'allocated\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})'`
   - `allocated`: Literal text indicating the start of the relevant section.
   - `\s+`: Matches one or more whitespace characters.
   - `\$\s*`: Captures the dollar sign followed by optional whitespace.
   - `(\d+(?:,\d{3})*\.\d{2})`: Captures a monetary value with comma-separated thousands and two decimal places.

6. **Miscellaneous Abbreviated Account Number**: `r"utility acct \*\*\*\*(\d+):"`
   - `utility acct`: Literal text indicating the start of the account number section.
   - `\*\*\*\*`: Matches exactly four asterisk characters.
   - `(\d+)`: Captures one or more digits.

7. **Miscellaneous kWh**: `r"\*{4}(\d+):(\d+)\s*kWh"`
   - `\*{4}`: Matches exactly four asterisk characters.
   - `(\d+)`: First capture group for a numeric value (account number).
   - `(\d+)\s*kWh`: Second capture group for kWh value followed by `kWh`.

8. **Miscellaneous Credits**: `r"\$(\d+(?:,\d{3})*\.\d{2})\s*bill credits"`
   - `\$(\d+(?:,\d{3})*\.\d{2})`: Captures a monetary value with comma-separated thousands and two decimal places.
   - `\s*bill credits`: Literal text indicating the end of the bill credits section.

9. **Miscellaneous Prices**: `r'bill credits allocated @ \$\s*(\d+(?:,\d{3})*\.\d{2})\s+\$\s*(\d+(?:,\d{3})*\.\d{2})'`
   - `bill credits allocated @ \$\s*`: Literal text indicating the start of the price section.
   - `(\d+(?:,\d{3})*\.\d{2})`: Captures a monetary value with comma-separated thousands and two decimal places.
   - Another `(\d+(?:,\d{3})*\.\d{2})`: Captures a second monetary value in the same format.

## [`etl.py`](utils/etl.py)

### Overview
The `etl.py` script is a comprehensive tool in the Electric Brew project, orchestrating the full spectrum of the ETL (Extract, Transform, Load) process. It integrates all the end-to-end operations from initial data extraction to preparing data for advanced analytics. This script interfaces with the `curation`, `modeling`, and `runtime` modules, ensuring a smooth flow of data through each stage.

### Script Execution Flow
The script's execution is categorized into distinct directories reflecting each stage of the ETL process:

- **RAW DATA EXTRACTION** (`/raw/parquet/`): Scrapes data from CMP and Ampion.
- **DATA CURATION** (`/curated/`): Transforms raw data into structured, query-ready formats.
- **DATA MODELING** (`/modeled/`): Builds the dimensional and fact tables essential for analysis.
- **DATABASE INTEGRATION** (`/sql/`): Connects to DuckDB, setting up the database environment.

### Detailed Function Descriptions
- **Raw Data Extraction**: 
  - `scrape_cmp_bills()`: Captures raw billing data from CMP, including key details such as billing periods and amounts.
  - `scrape_ampion_bills()`: Retrieves raw billing data from Ampion, with a focus on renewable energy credits.

- **Data Curation**: 
  - `write_results(load_data_files())`: Executes a dual process of loading and transforming raw data into a curated format. This step is applied across various datasets, including CMP's meter usage and location data, as well as billing information from both CMP and Ampion.

- **Data Modeling**: 
  - `model_dim_datetimes()`: Breaks down timestamps into date and time components, and classifies periods of the day for peak hour analysis.
  - `model_dim_meters()`: Collates key meter information and location details, crucial for a comprehensive view of energy consumption across accounts.
  - `model_dim_bills()`: Merges intricate billing details from CMP and Ampion, vital for a deep dive into energy costs, delivery rates, and supplier nuances.
  - `model_fct_electric_brew()`: Creates the `fct_electric_brew` fact table, synthesizing electricity usage and costs at a granular level, key for profitability analysis and consumption pattern insights.

Running the `etl.py` script from the project's root directory processes all data through these stages, ensuring the Electric Brew project's data is continuously primed for insightful analytics and reporting.

## [`modeling.py`](utils/modeling.py)

This section comprises functions that transform DataFrames into a structured, denormalized data model optimized for analytical queries and data visualization. It includes the generation of dimensional tables and the enhancement of timestamp data to facilitate intuitive querying.

### `model_dim_datetimes`

Generates a datetime dimension table, which is a key component in time series analysis and reporting. It enriches the dataset by breaking down timestamps into more granular and useful components, facilitating more sophisticated temporal queries and analyses.

**Methodology**

1. Extract and sort unique timestamps from `meter_usage['interval_end_datetime']`.
2. Decompose timestamps into individual time components.
3. Categorize timestamps into time periods based on the hour of the day.
4. Assign a unique identifier `id` to each timestamp.
5. Persist the resulting dataframe as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file saved in the specified `modeled` directory containing the datetime dimension table.

**Example Output**

| id      | timestamp           | increment | hour | date       | week | week_in_year | month | month_name | quarter | year | period                                |
|---------|---------------------|-----------|------|------------|------|--------------|-------|------------|---------|------|---------------------------------------|
| 1       | 2020-10-08 00:00:00 | 0         | 0    | 2020-10-08 | 41   | 41           | 10    | October    | 4       | 2020 | Off-peak: 12AM to 7AM                 |
| 2       | 2020-10-08 00:15:00 | 15        | 0    | 2020-10-08 | 41   | 41           | 10    | October    | 4       | 2020 | Off-peak: 12AM to 7AM                 |
| 3       | 2020-10-08 00:30:00 | 30        | 0    | 2020-10-08 | 41   | 41           | 10    | October    | 4       | 2020 | Off-peak: 12AM to 7AM                 |
| 4       | 2020-10-08 00:45:00 | 45        | 0    | 2020-10-08 | 41   | 41           | 10    | October    | 4       | 2020 | Off-peak: 12AM to 7AM                 |
| 5       | 2020-10-08 01:00:00 | 0         | 1    | 2020-10-08 | 41   | 41           | 10    | October    | 4       | 2020 | Off-peak: 12AM to 7AM                 |
| ...     | ...                 | ...       | ...  | ...        | ...  | ...          | ...   | ...        | ...     | ...  | ...                                   |
| 104432  | 2023-09-30 22:45:00 | 45        | 22   | 2023-09-30 | 39   | 39           | 9     | September  | 3       | 2023 | Mid-peak: 7AM to 5PM, 9PM to 11PM     |
| 104433  | 2023-09-30 23:00:00 | 0         | 23   | 2023-09-30 | 39   | 39           | 9     | September  | 3       | 2023 | On-peak: 5PM to 9PM                   |
| 104434  | 2023-09-30 23:15:00 | 15        | 23   | 2023-09-30 | 39   | 39           | 9     | September  | 3       | 2023 | On-peak: 5PM to 9PM                   |
| 104435  | 2023-09-30 23:30:00 | 30        | 23   | 2023-09-30 | 39   | 39           | 9     | September  | 3       | 2023 | On-peak: 5PM to 9PM                   |
| 104436  | 2023-09-30 23:45:00 | 45        | 23   | 2023-09-30 | 39   | 39           | 9     | September  | 3       | 2023 | On-peak: 5PM to 9PM                   |

### `model_dim_meters`

Creates a meters dimension table, which centralizes the account information, linking service points, meter IDs, and location details. This table simplifies the complexity of account management and enables more efficient meter-related queries and analyses. This table originally was curated for accounts, but through the discovery process, we learned that it's possible for a given account to have multiple meters and service points.

**Methodology**

1. Read the `meter_usage` and `locations` DataFrames.
2. Extract and join relevant columns based on `account_number`.
3. Assign a unique identifier `id` to each account entry.
4. Persist the resulting dataframe as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file saved in the specified `modeled` directory containing the accounts dimension table.

**Example Output**

| id | meter_id   | service_point_id | account_number | street               | label          |
|----|------------|------------------|----------------|----------------------|----------------|
| 1  | L108605388 | 2300822246       | 30010320353    | 115 FOX ST UNIT 115  | Fox Street     |
| 2  | L108558642 | 2300822209       | 30010320361    | 115 FOX ST UNIT 103  | Fox Street     |
| 3  | L108557737 | 2300910019       | 30010601281    | 111 FOX ST UNIT 2    | Fox Street     |
|... | ...        | ...              | ...            | ...                  | ...            |
| 8  | L108607371 | 2300588897       | 35012790198    | 1 INDUSTRIAL WAY U10 | Industrial Way |

### `model_dim_bills`

Creates a comprehensive bills dimension table (`dim_bills`), consolidating detailed billing information from the `cmp_bills` and `ampion_bills` DataFrames. This table is pivotal for analyzing billing data across various dimensions such as invoice number, account number, and supplier, and includes metrics like delivered kWh, service charges, taxes, delivery rates, and supply rates.

**Methodology**

1. Group `cmp_bills` and `ampion_bills` by common dimensions (`invoice_number`, `account_number`, `interval_start`, `interval_end`, `supplier`) and aggregate necessary metrics.
2. Concatenate the results from both DataFrames, assigning a source identifier for each row.
3. Replace `interval_start`, `interval_end` fields with a `billing_interval` field, representing the inclusive range of dates for each billing period.
4. Create a unique identifier `id` for each row.
5. Persist the combined DataFrame as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file in the specified `modeled` directory containing the `dim_bills` dimensional table.

**Example Output**

| id  | invoice_number    | account_number | supplier              | kwh_delivered | service_charge | taxes  | delivery_rate | supply_rate | source | billing_interval                              |
|-----|-------------------|----------------|-----------------------|---------------|----------------|--------|---------------|-------------|--------|-----------------------------------------------|
| 1   | 700000396769      | 30010320353    |                       | 4522          | 21.47          | 20.51  | 0.077711      | NaN         | CMP    | [2021-12-21 ... 2022-01-20]                    |
| 2   | 700000447768      | 30010320353    | MEGA ENERGY OF MAINE  | 914           | 21.47          | 8.53   | 0.077713      | 0.068403    | CMP    | [2022-05-18 ... 2022-06-17]                    |
| 3   | 701001427136      | 30010320353    |                       | 2989          | 21.47          | 13.96  | 0.077712      | NaN         | CMP    | [2021-10-19 ... 2021-11-18]                    |
| 4   | 701001458379      | 30010320353    |                       | 5635          | 21.47          | 25.27  | 0.077711      | NaN         | CMP    | [2021-11-17 ... 2021-12-16]                    |
| 5   | 701001542641      | 30010320353    |                       | 4439          | 21.47          | 20.15  | 0.077711      | NaN         | CMP    | [2022-02-18 ... 2022-03-19]                    |
| ... | ...               | ...            | ...                   | ...           | ...            | ...    | ...           | ...         | ...    | ...                                           |
| 263 | 2023100000830629  | 30010601281    | Ampion                | 1968          | 0.00           | 0.00   | 0.000000      | 0.205066    | Ampion | [2023-07-13 ... 2023-08-12]                    |
| 264 | 2023100000830629  | 30010894035    | Ampion                | 3633          | 0.00           | 0.00   | 0.000000      | 0.205070    | Ampion | [2023-07-13 ... 2023-08-12]                    |
| 265 | 2023100000830629  | 35012787137    | Ampion                | 722           | 0.00           | 0.00   | 0.000000      | 0.205069    | Ampion | [2023-07-13 ... 2023-08-12]                    |
| 266 | 2023100000830629  | 35012787756    | Ampion                | 759           | 0.00           | 0.00   | 0.000000      | 0.205059    | Ampion | [2023-07-13 ... 2023-08-12]                    |
| 267 | 2023100000830629  | 35012790198    | Ampion                | 2092          | 0.00           | 0.00   | 0.000000      | 0.206902    | Ampion | [2023-07-13 ... 2023-08-12]                    |

**Note**: The 'billing_interval' column specifies the inclusive start and end dates for each billing period, providing a detailed timeline for each billing record.

### `model_fct_electric_brew`

This function constructs the `fct_electric_brew` fact table, a cornerstone of the analytics model for Austin Street Brewery's electricity consumption and cost analysis. This table integrates and transforms data from various sources, capturing detailed electric usage and associated charges for each customer account at specific time intervals.

**Methodology**

1. Expand billing intervals from `cmp_bills` and `ampion_bills` to daily granularity, grouping them by source and aligning with daily meter usage data from `meter_usage`. This ensures precise association of daily charges with corresponding usage data.
2. Create an intermediary DataFrame by merging the expanded billing data with `meter_usage`, alongside `dim_meters` and `dim_datetimes`.
3. Calculate total kWh recorded for each invoice number and kWh delivered, enabling the proportional allocation of service charges and taxes based on actual usage.
4. Sort CMP billing data by invoice number and timestamp, computing cumulative metrics for remaining and used kWh in reverse order for each interval.
5. Apply a similar calculation for Ampion billing, but start from the beginning of each interval to determine used and remaining kWh in ascending order.
6. Calculate delivery, service, and supply costs based on the used kWh, reflecting the various cost components associated with electricity delivery and usage.
7. Assemble the final fact table with all required fields, assigning a unique identifier `id` to each row as a primary key.
8. Save the table as a `.parquet` file in the specified `modeled` directory, utilizing `snappy` compression and partitioning by `account_number` for enhanced storage and query efficiency.

**Returns**

A `.parquet` file saved in the specified `modeled` directory containing the `fct_electric_brew` table.

**Example Output**

| id     | dim_datetimes_id | dim_meters_id | dim_bills_id | kwh   | delivery_cost | service_cost | supply_cost | tax_cost  | total_cost | account_number |
|--------|------------------|---------------|--------------|-------|---------------|--------------|-------------|-----------|------------|----------------|
| 1      | 69401            | 1             | 191.0        | 0.594 | 0.000000      | 0.008452     | 0.099173    | 0.000464  | 0.108089   | 30010320353    |
| 2      | 69402            | 1             | 191.0        | 0.101 | 0.000000      | 0.001437     | 0.016863    | 0.000079  | 0.018379   | 30010320353    |
| 3      | 69403            | 1             | 191.0        | 0.104 | 0.000000      | 0.001480     | 0.017364    | 0.000081  | 0.018925   | 30010320353    |
| 4      | 69404            | 1             | 191.0        | 0.106 | 0.000000      | 0.001508     | 0.017697    | 0.000083  | 0.019289   | 30010320353    |
| 5      | 69405            | 1             | 191.0        | 0.099 | 0.000000      | 0.001409     | 0.016529    | 0.000077  | 0.018015   | 30010320353    |
| ...    | ...              | ...           | ...          | ...   | ...           | ...          | ...         | ...       | ...        | ...            |
| 501330 | 34341            | 8             | 183.0        | 1.284 | 0.098879      | 0.026688     | NaN         | 0.006957  | NaN        | 35012790198    |
| 501331 | 34343            | 8             | 183.0        | 1.260 | 0.097572      | 0.026360     | NaN         | 0.006867  | NaN        | 35012790198    |
| 501332 | 34345            | 8             | 183.0        | 1.242 | 0.096517      | 0.025867     | NaN         | 0.006735  | NaN        | 35012790198    |
| 501333 | 34347            | 8             | 183.0        | 1.220 | 0.095231      | 0.025505     | NaN         | 0.006644  | NaN        | 35012790198    |
| 501334 | 34349            | 8             | 183.0        | 1.202 | 0.093409      | 0.025034     | NaN         | 0.006518  | NaN        | 35012790198    |
| ...    | ...              | ...           | ...          | ...   | ...           | ...          | ...         | ...       | ...        | ...            |

## [`runtime.py`](utils/runtime.py)

This section contains functions primarily focused on setting up and configuring the environment for data visualization and data reading. These functions make sure that all plots have a uniform appearance and that data files can be easily read into Pandas DataFrames.

### `set_plot_params`

**Purpose**  
Initializes and returns custom plotting parameters for `matplotlib`, ensuring consistent visual style throughout the project.

**Signature** 
```python
def set_plot_params() -> list:
```

**Returns**  
A list containing RGBA color tuples that comprise the custom color palette for plots.

### `read_data`

**Purpose**  
Reads `.parquet` files into Pandas DataFrames. The function resolves the path relative to the `data` directory of the project, no matter where your script is located within the `src` directory or where you've cloned the repo. As such, it expects a file path string that starts from within the `data` directory.

**Signature** 
```python
def read_data(file_path : str) -> pd.DataFrame:
```

**Returns**  
A DataFrame containing the data read from the supplied Parquet file path.

### `connect_to_db`

**Purpose**  
Establishes a connection to the `electric_brew` DuckDB database and creates SQL views based on Parquet files for the Electric Brew project. It facilitates direct querying of Parquet files, ensuring real-time data reflection in the views and simplifying data access across the project.

**Signature** 
```python
def connect_to_db(db  : dd.DuckDBPyConnection = dd.connect('./data/sql/electric_brew.db'),
                  vws : dict = {'meter_usage'       : 'cmp/curated/meter_usage',
                                'locations'         : 'cmp/curated/locations',
                                'cmp_bills'         : 'cmp/curated/bills',
                                'ampion_bills'      : 'ampion/curated',
                                'dim_datetimes'     : 'modeled/dim_datetimes',
                                'dim_meters'        : 'modeled/dim_meters',
                                'dim_bills'         : 'modeled/dim_bills',
                                'fct_electric_brew' : 'modeled/fct_electric_brew'}) -> dd.DuckDBPyConnection:
```

**Methodology**

1. **Database Connection**: Establishes or opens a connection to the specified DuckDB database.
2. **View Creation**: Iterates through a mapping of view names to Parquet file paths, creating a SQL view for each. If a view exists, it proceeds without interruption.

**Returns**  
The function returns a `duckdb.DuckDBPyConnection` object, representing the connected DuckDB database instance.

The database itself is accessible in any `/src/` script via the `electric_brew` variable. This standardized access point ensures uniformity and convenience in database interactions throughout the project.

A comprehensive Entity-Relationship Diagram (ERD) of the database can be found in the `sql` directory's [README](../data/sql/README.md). This ERD provides a visual representation of the tables, their schemas, and the relationships between each.

## [`variables.py`](utils/variables.py)

### Overview
This script serves as a central repository for initializing key DataFrames and a DuckDB database connection used throughout the Electric Brew project. By centralizing these data structures, the script promotes efficiency, consistency, and code reusability across various components of the project. It ensures that data is accessed in an optimized manner, leveraging pre-curated and modeled data sets.

### DataFrames and Database Initialization
Several `pandas` DataFrames are initialized as variables, each representing a specific aspect of the project's data model. These DataFrames are categorized into 'Curated DataFrames' and 'Modeled DataFrames,' reflecting their stage in the data pipeline. Additionally, a DuckDB database connection is established to facilitate SQL-based data operations on top of all the defined DataFrames below.

#### Curated DataFrames
- `meter_usage`: Contains kWh readings from Central Maine Power (CMP), offering granular insight into electricity usage in as frequent as 15-minute intervals.
- `locations`: Enriches the dataset with manually curated CSV entries, detailing Austin Street's account locations and relevant metadata.
- `cmp_bills`: Houses CMP's billing data, including delivery and supplier rates for various periods, essential for financial analysis.
- `ampion_bills`: Captures billing data from Austin Street's solar provider, Ampion, detailing kWh supplied and associated pricing.

#### Modeled DataFrames
- `dim_datetimes`: Breaks down timestamps into individual date and time components, aiding in detailed temporal analysis.
- `dim_meters`: Consolidates account numbers, service points, and location details into a singular dimensional table.
- `dim_bills`: Unifies key dimensions and metrics from both `cmp_bills` and `ampion_bills`, providing a comprehensive billing overview.
- `fct_electric_brew`: The central fact table that encapsulates detailed records of electricity usage, billing, and delivery costs.

#### DuckDB Database Connection
- `electric_brew`: Establishes a DuckDB database connection, containing pointer views to all the above DataFrames. This connection is instrumental for executing complex SQL queries directly on the DataFrame data, enhancing the project's data handling capabilities.

### Data Dictionary
For an in-depth understanding of each DataFrame, including field descriptions, data types, and their curation sources, refer to the [**data dictionary**](../docs/data_dictionary.md). This document offers a detailed blueprint of the data structure and schema used in the Electric Brew project.
