<!-- omit in toc -->
# Utility Functions

The `/src/` directory contains a `utils` directory full of scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. This document provides a brief overview of each function and its purpose, signature, and expected usage.

> **Note**: To facilitate smooth development and execution, it's recommended to run all commands out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** is set to point directly to the `/src/` directory within this Conda environment. This allows you to easily import any function or variable within the `utils` module into any script within `/src/`.

<!-- omit in toc -->
## Table of Contents
- [`curation.py`](#curationpy)
  - [`curate_meter_usage`](#curate_meter_usage)
    - [**Regular Expressions**](#regular-expressions)
    - [**Manual Interventions**](#manual-interventions)
  - [`scrape_ampion_bills`](#scrape_ampion_bills)
    - [**Regular Expressions**](#regular-expressions-1)
- [`etl.py`](#etlpy)
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


## [`curation.py`](utils/curation.py)

This section comprises functions that transform raw data files into structured and query-optimized formats. This includes converting raw CSVs into partitioned Parquet files and extracting relevant data from PDFs.

### `curate_meter_usage`

Note: the documentation for this section will be reworked, since the paradigm for each `curate_` function is essentially the same, except for the addition of `schema` in this first one.

Processes all CSV files from the provided directory, integrates the specified schema, and subsequently consolidates the data into a partitioned `.parquet` file in the designated output directory. This conversion and curation process is optimized with `snappy` compression for efficiency.

**Signature** 
```python
def curate_meter_usage(raw           : str, 
                       curated       : str, 
                       partition_col : list, 
                       schema        : list):
```

**Parameters**

- **`raw`**: Path to the directory containing raw `meter_usage` CSV files.

- **`curated`** : Directory where the consolidated `.parquet` files will be saved.

- **`partition_col`**: Columns by which the .parquet files will be partitioned.

- **`schema`**: Columns that will be used as headers in the resulting DataFrame.
 
This function automates the extraction of specific fields from a collection of PDF bills stored in a directory. It is designed to capture around 90% of the values from these bills. However, due to variations in the format and content of individual documents, manual review and intervention is required for complete accuracy.

**Signature** 
```python
def scrape_cmp_bills(raw    : str, 
                     output : str):
```

**Parameters**

- **`raw`**: Path to the directory containing the PDF bills that need to be processed.

- **`output`** : Path where the extracted data will be saved as a CSV file.

#### **Regular Expressions**

The `scrape_cmp_bills` function uses various regular expressions (RegEx) to identify and extract specific pieces of information from text content within utility bills stored as PDF files. Below, each RegEx is broken down to explain its components and what it aims to capture. Patterns were detected and tested using [**RegExr**](https://regexr.com), a visual IDE for finding RegEx patterns within blocks of text.

1. **Account Number**: `r"Account Number\s*([\d-]+)"`
    - `Account Number`: Literal text that the RegEx searches for.
    - `\s*`: Matches zero or more whitespace characters.
    - `([\d-]+)`: Captures one or more digits or dashes.
    
    **Purpose**: Captures the account number that follows the literal text "Account Number", allowing for possible whitespace and dashes.

2. **Amount Due**: `r"Amount Due Date Due\s*\d+-\d+-\d+ [A-Z\s]+ \$([\d,]+\.\d{2})"`
    - `Amount Due Date Due`: Literal text that the RegEx searches for.
    - `\s*`: Matches zero or more whitespace characters.
    - `\d+-\d+-\d+`: Captures a date in the format `d+-d+-d+`.
    - `[A-Z\s]+`: Captures one or more uppercase letters or whitespace.
    - `\$\s*`: Captures the dollar sign and any following whitespace.
    - `([\d,]+\.\d{2})`: Captures one or more digits or commas, followed by a decimal and exactly two digits.

    **Purpose**: Finds the amount due that follows the literal text "Amount Due Date Due", capturing the date and any uppercase letters or whitespace, accounting for optional whitespace, and ensures the amount has two decimal places.

3. **Service Charge**: `r"Service Charge.*?@\$\s*([+-]?\d+\.\d{2})"`
    - `Service Charge`: Literal text that the RegEx starts with.
    - `.*?`: Lazily matches any number of any characters.
    - `@\$\s*`: Captures the '@$' symbol followed by any number of whitespace characters.
    - `([+-]?\d+\.\d{2})`: Captures a number that may be positive or negative, followed by a decimal and exactly two digits.
    
    **Purpose**: Captures the service charge value found after the term "Service Charge", accounting for both positive and negative values and ensuring two decimal places.

4. **Delivery Service Rate**: `r"Delivery Service[:\s]*\d+,?\d+ KWH @\$(\d+\.\d+)"`
    - `Delivery Service`: Literal text to start the RegEx.
    - `[:\s]*`: Captures a colon or any number of whitespace characters.
    - `\d+,?\d+ KWH`: Captures one or more digits, optionally a comma, and more digits followed by ' KWH'.
    - `@\$(\d+\.\d+)`: Captures the '@$' symbol followed by one or more digits, a decimal point, and more digits.
    
    **Purpose**: Extracts the rate per kilowatt-hour (kWh) for delivery service, following the term "Delivery Service".

5. **Meter Details**: `r"Delivery Charges.*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,4},?\d{0,3}) KWH.*?Total Current Delivery Charges"`
    - `Delivery Charges.*?`: Starts with the literal text and lazily matches any number of any characters.
    - `(\d{1,2}/\d{1,2}/\d{4})`: Captures a date in the format MM/DD/YYYY.
    - `.*?`: Lazily matches any number of any characters.
    - `(\d{1,4},?\d{0,3}) KWH`: Captures one or more digits, optionally a comma, and more digits followed by ' KWH'.
    - `.*?Total Current Delivery Charges`: Lazily matches any number of any characters until it finds the text 'Total Current Delivery Charges'.

    **Purpose**: Captures multiple details (interval start, interval end, and kilowatt-hours (kWh) delivered) within the section starting with "Delivery Charges".

#### **Manual Interventions**

After the automated scraping process is completed by `scrape_cmp_bills`, several manual inputs are generally required to ensure data accuracy and completeness. Here are the steps for those interventions:

1. **File Relocation and Renaming**:  
    - **Initial State**: A CSV file named `scraped_bills.csv` is generated in the `cmp/raw/bills` directory.  
    - **Action**: Rename this file to `scraped_bills_with_edits` to avoid accidental overwrites of manually curated data.

2. **Handling Null 'Amount Due'**:  
    - **Scenario**: Sometimes the `amount_due` field is NULL if the bill is fully paid off.  
    - **Action**: Manually enter a 0 for such cases.  
    - **Example**: See [`30010320353/700000447768_bill.pdf`](../data/cmp/raw/bills/30010320353/700000447768_bill.pdf).

3. **Managing Multiple Billing Intervals**:  
    - **Scenario**: Some bills may have more than one billing interval, often crossing fiscal quarters, and the function can only pick up one.
    - **Action**: Add an additional row in the CSV with the same `account_number`, `amount_due`, and `pdf_file_name`, but with differing values for other fields, like `kwh_delivered`  
    - **Example**: See [`30010320353/703001515406_bill.pdf`](../data/cmp/raw/bills/30010320353/703001515406_bill.pdf) and [`30010320353/702001847715_bill.pdf`](../data/cmp/raw/bills/30010320353/702001847715_bill.pdf).

4. **Addressing Missing 'Delivery Service Rate'**:  
    - **Scenario**: Some bills do not include a `delivery_service_rate`.  
    - **Action**: Leave this field and `kwh_delivered` as NULL, with the understanding that the delivery was covered by previous `Banked Generation`.  
    - **Example**: See [`30010320353/705001871139_bill.pdf`](../data/cmp/raw/bills/30010320353/705001871139_bill.pdf).

5. **External Electricity Supply**:  
    - **Scenario**: In cases where electricity is supplied by an external provider.  
    - **Action**: Manually add the `supplier` and `supply_rate`.
    - **Example**: See [`30010320353/701001868909_bill.pdf`](../data/cmp/raw/bills/30010320353/701001868909_bill.pdf).

6. **Record Total kWh**:  
    - **Scenario**: This columns in each bill's **Meter Details** might be located in different places.
    - **Action**: Manually add `total_kwh` for each new bill.

After these manual interventions are performed, the curated CSV file is ready for conversion into Parquet format for further data processing.

### `scrape_ampion_bills`
 
This function automates the extraction of specific fields from a collection of PDF bills stored in a directory. In this instance, the design of the regular expression captures all patterns successfully. In addition, there are some conditional measures to respond to the location of visible elements like the "Your Price" banner. A manual intervention is performed for "Miscellaneous Charges" that do not fit the structure of the bills.

**Signature** 
```python
def scrape_ampion_bills(raw    : str, 
                        output : str):
```

**Parameters**

- **`raw`**: Path to the directory containing the raw PDF utility bills that need to be processed.

- **`output`**: Path where the extracted data will be saved as CSV files.

#### **Regular Expressions**

The `scrape_ampion_bills` function employs various regular expressions (RegEx) to identify and extract specific pieces of information from the text content within the PDF utility bills. Below, each RegEx is broken down to explain its components and what it aims to capture:

1. **Invoice Number**: `r"Invoice:\s(\d+)"`
   - `Invoice:`: Literal text that the RegEx searches for.
   - `\s`: Matches a single whitespace character.
   - `(\d+)`: Captures one or more digits.
   
   **Purpose**: Extracts the invoice number immediately following the literal text "Invoice:".

2. **Abbreviated Account Number**: `r'\*{5}(\d+)'`
   - `\*{5}`: Matches exactly five asterisk characters.
   - `(\d+)`: Captures one or more digits.
   
   **Purpose**: Captures the account number that follows five asterisks, typically representing obfuscated digits in an account number.

3. **Kilowatt-Hour (kWh) Values**: `r'(\d{1,4}(?:,\d{3})*?) kWh'`
   - `(\d{1,4}(?:,\d{3})*?)`: Captures a numeric value, allowing for comma-separated thousands. The `?` makes the match lazy.
   - `kWh`: Literal text indicating the unit of energy.
   
   **Purpose**: Extracts energy usage values in kilowatt-hours from the bill.

4. **Prices**: `r'allocated\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})\s+\$ (\d+(?:,\d{3})*\.\d{2})'`
   - `allocated`: Literal text indicating the start of the relevant section.
   - `\s+`: Matches one or more whitespace characters.
   - `\$\s*`: Captures the dollar sign followed by optional whitespace.
   - `(\d+(?:,\d{3})*\.\d{2})`: Captures a monetary value, potentially with comma-separated thousands and exactly two decimal places.
   
   **Purpose**: Extracts different price values (allocated prices) from the bill.

5. **Date Ranges**: `r'(\d{2}\.\d{2}\.\d{4})\s*–\s*(\d{2}\.\d{2}\.\d{4})'`
   - `(\d{2}\.\d{2}\.\d{4})`: Captures a date in the format DD.MM.YYYY.
   - `\s*–\s*`: Matches a dash surrounded by optional whitespace.
   - Another `(\d{2}\.\d{2}\.\d{4})`: Captures a second date in the same format.
   
   **Purpose**: Extracts the start and end dates of the billing period.

## [`etl.py`](utils/etl.py)

Reproducibility script for taking all of the landed raw files and passing them through each stage of the landing zone architecture, until the data is ready for analytics. Will add more detail here.

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

Creates a bills dimension table, which consolidates billing information from both `cmp_bills` and `ampion_bills` DataFrames into a comprehensive view. This table is instrumental for analyzing combined billing data across various dimensions, such as invoice number, account number, and supplier, and includes metrics like delivered kWh, service charges, delivery rates, and supply rates.

**Methodology**

1. Group `cmp_bills` and `ampion_bills` by common dimensions (`invoice_number`, `account_number`, `interval_start`, `interval_end`, `supplier`) and aggregate necessary metrics.
2. Concatenate the results from both DataFrames and assign a source identifier for each row.
3. Replace the `interval_start`, `interval_end` fields with a `billing_interval`` field, representing the inclusive range of dates for each billing period.
4. Create a unique identifier `id` for each row.
5. Persist the combined DataFrame as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file saved in the specified `modeled` directory containing the combined bills dimension table.

**Example Output**

| id  | invoice_number      | account_number | supplier              | kwh_delivered | service_charge | delivery_rate | supply_rate | source | billing_interval                                  |
|-----|---------------------|----------------|-----------------------|---------------|----------------|---------------|-------------|--------|---------------------------------------------------|
| 1   | 700000396769        | 30010320353    | Mega Energy of Maine  | 4522.0        | 21.47          | 0.077711      | 0.068400    | CMP    | [2021-12-21, 2021-12-22, ..., 2022-01-19]         |
| 2   | 700000447767        | 30010320361    | Mega Energy of Maine  | 470.0         | 21.47          | 0.077711      | 0.068400    | CMP    | [2022-05-18, 2022-05-19, ..., 2022-06-10]         |
| 3   | 700000447768        | 30010320353    | Mega Energy of Maine  | 914.0         | 21.47          | 0.077711      | 0.068400    | CMP    | [2022-05-18, 2022-05-19, ..., 2022-06-10]         |
| ... | ...                 | ...            | ...                   | ...           | ...            | ...           | ...         | ...    | ...                                               |
| 161 | 2023100000830629    | 30010601281    | Ampion                | 1968.0        | 0.00           | 0.000000      | 0.205066    | Ampion | [2023-07-13, 2023-07-14, ..., 2023-08-13]         |
| 162 | 2023100000830629    | 30010894035    | Ampion                | 3633.0        | 0.00           | 0.000000      | 0.205070    | Ampion | [2023-07-13, 2023-07-14, ..., 2023-08-13]         |
| 163 | 2023100000830629    | 35012787137    | Ampion                | 722.0         | 0.00           | 0.000000      | 0.205069    | Ampion | [2023-07-13, 2023-07-14, ..., 2023-08-13]         |
| ... | ...                 | ...            | ...                   | ...           | ...            | ...           | ...         | ...    | ...                                               |

**Note**: The 'billing_interval' column is a list of dates, covering the entire duration of the billing period, which provides a detailed view of the billing timeline for each record.

### `model_fct_electric_brew`

This function constructs the `fct_electric_brew` fact table, a cornerstone of the analytics model for Austin Street Brewery's electricity consumption and cost analysis. This table integrates and transforms data from various sources, capturing detailed electric usage and associated charges for each customer account at specific time intervals.

**Methodology**

1. Billing intervals from `cmp_bills` and `ampion_bills` are expanded to daily granularity and grouped by their source, aligning them with daily meter usage data from `meter_usage`. This step ensures accurate association of daily charges with usage data.
2. An intermediary DataFrame is curated by merging the expanded billing data with `meter_usage`, along with dimension tables `dim_meters`, `dim_datetimes`, and billing data segregated by source. This provides a comprehensive dataset combining usage with billing information.
3. The total kWh recorded for each invoice number and kWh delivered is calculated, allowing for the proportional allocation of service charges based on usage.
4. For CMP billing, the data is sorted by invoice number and timestamp. Cumulative metrics for remaining and used kWh are calculated in reverse order, starting at the end of each interval.
5. For Ampion billing, a similar approach is taken, but the calculation starts from the beginning of each interval, computing remaining and used kWh in ascending order.
6. Delivery, service, and supply costs are calculated based on the used kWh. These metrics reflect the cost components associated with electricity delivery and usage.
7. The final fact table is assembled with all necessary fields and a unique identifier `id` is assigned to each row, providing a primary key.
8. The table is saved as a `.parquet` file in the specified `modeled` directory, with `snappy` compression and partitioned by `account_number` for optimized storage and query performance.

**Returns**

A `.parquet` file saved in the specified `modeled` directory containing the `fct_electric_brew` table.

**Example Output**

| id     | dim_datetimes_id | dim_meters_id | dim_bills_id | kwh   | delivery_cost | service_cost | supply_cost | total_cost | account_number |
|--------|------------------|---------------|--------------|-------|---------------|--------------|-------------|------------|----------------|
| 1      | 69401            | 1             | 190.0        | 0.594 | 0.000000      | 0.008452     | 0.099173    | 0.107625   | 30010320353    |
| 2      | 69402            | 1             | 190.0        | 0.101 | 0.000000      | 0.001437     | 0.016863    | 0.018300   | 30010320353    |
| 3      | 69403            | 1             | 190.0        | 0.104 | 0.000000      | 0.001480     | 0.017364    | 0.018843   | 30010320353    |
| 4      | 69404            | 1             | 190.0        | 0.106 | 0.000000      | 0.001508     | 0.017697    | 0.019206   | 30010320353    |
| 5      | 69405            | 1             | 190.0        | 0.099 | 0.000000      | 0.001409     | 0.016529    | 0.017938   | 30010320353    |
| ...    | ...              | ...           | ...          | ...   | ...           | ...          | ...         | ...        | ...            |
| 500276 | 34345            | 8             | 153.0        | 1.242 | 0.096517      | 0.025867     | 0.000000    | 0.122384   | 35012790198    |
| 500277 | 34349            | 8             | 153.0        | 1.202 | 0.093409      | 0.025034     | 0.000000    | 0.118443   | 35012790198    |
| 500278 | 34353            | 8             | 153.0        | 1.186 | 0.092165      | 0.024701     | 0.000000    | 0.116866   | 35012790198    |
| 500279 | 34357            | 8             | 153.0        | 1.150 | 0.089368      | 0.023951     | 0.000000    | 0.113319   | 35012790198    |
| 500280 | 34361            | 8             | 153.0        | 1.120 | 0.087036      | 0.023326     | 0.000000    | 0.110363   | 35012790198    |

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
                                'ampion_bills'      : 'ampion/curated/bills',
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

This section initializes commonly used DataFrames (and their supporting database), making them readily available across different parts of the project. This promotes code reusability and performance optimization.

For detailed descriptions of the fields, their data types, and the files responsible for thier curation, visit our [**data dictionary**](../docs/data_dictionary.md) document.