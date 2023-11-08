<!-- omit in toc -->
# Directory Documentation for `/src/`

The `/src/` directory contains various utility scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. Our instance of `/src/` houses scripts responsible for data curation, output formatting, and visualization. This document provides a brief overview of each script and its contained functions.

> **Note**: To facilitate smooth development and execution, it's recommended to run all commands out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** is set to point directly to the `src` directory within this Conda environment. This allows you to easily import the `utils` module and its DataFrames and functions from any script within the `src` directory.

<!-- omit in toc -->
## Table of Contents
- [Runtime](#runtime)
  - [`set_plot_params`](#set_plot_params)
  - [`read_data`](#read_data)
- [DataFrames](#dataframes)
  - [`meter_usage`](#meter_usage)
  - [`locations`](#locations)
  - [`cmp_bills`](#cmp_bills)
  - [`dim_datetimes`](#dim_datetimes)
  - [`dim_accounts`](#dim_accounts)
  - [`dim_suppliers`](#dim_suppliers)
- [Curation](#curation)
  - [`curate_meter_usage`](#curate_meter_usage)
  - [`curate_cmp_bills`](#curate_cmp_bills)
  - [`scrape_cmp_bills`](#scrape_cmp_bills)
    - [**Regular Expressions**](#regular-expressions)
    - [**Manual Interventions**](#manual-interventions)
- [Modeling](#modeling)
  - [`create_dim_datetimes`](#create_dim_datetimes)
  - [`create_dim_meters`](#create_dim_meters)
  - [`create_dim_suppliers`](#create_dim_suppliers)


## Runtime

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


## DataFrames

This section initializes commonly used DataFrames at the start, making them readily available across different parts of the project. This promotes code reusability and performance optimization.

### `meter_usage`

A repository for meter-level electrical consumption data from Central Maine Power (CMP) in 15-minute intervals. Used in analyses of electricity usage patterns, billing, and location-related insights. The DataFrame is partitioned by `account_number`, enabling quick data retrieval for individual accounts. 

**Source**: Central Maine Power (CMP)  
**Location**: `./data/cmp/curated/meter-usage`  
**Partitioning**: `account_number`  

**Schema**:

  - `service_point_id` (**int**): A unique identifier for the point where the electrical service is provided, often tied to a specific location or customer.
  
  - `meter_id` (**str**): Identifier for the electrical meter installed at the service point. It records the amount of electricity consumed.
  
  - `interval_end_datetime` (**str**): Timestamp marking the end of the meter reading interval, typically indicating when the meter was read.
  
  - `meter_channel` (**int**): The channel number on the electrical meter. Meters with multiple channels can record different types of data.
  
  - `kwh` (**float**): Kilowatt-hours recorded by the meter during the interval, representing the unit of electricity consumed.

  - `account_number` (**int**): A unique identifier assigned by Central Maine Power for the customer's account. Used for all billing and service interactions.

### `locations`

A DataFrame that contains location-based information for CMP accounts, linking street addresses to account numbers. It is essential for correlating energy consumption with specific locations and their equipment.

**Source**: Manual Entry
**Location**: `./data/cmp/curated/locations`  
**Partitioning**: `account_number`  

**Schema**:

  - `street` (**str**): The street address associated with the CMP account, detailing the exact location.
  
  - `label` (**str**): A simplified or common name label for the location, which may be used for easier reference.
  
  - `account_number` (**int**): A unique identifier assigned by Central Maine Power for the customer's account, linking the location to the specific account for billing and service interactions.

### `cmp_bills`

A consolidated view of billing information from various suppliers for Central Maine Power (CMP) accounts. The DataFrame is partitioned by `account_number`, making it easy to retrieve data for specific accounts quickly.

**Source**: Central Maine Power (CMP)  
**Location**: `./data/cmp/curated/bills`  
**Partitioning**: `account_number`  

**Schema**:

  - `supplier` (**str**): The electricity supplier for the billing period. This is often a third-party energy supplier, though some billing periods use "Banked Generation" and credits from prebious supplier purchases.
  
  - `amount_due` (**float**): The total monetary amount due for the billing period. This includes all charges, fees, and taxes.
  
  - `service_charge` (**float**): A volume-based fee charged for delivery service through the electrical grid by CMP.
  
  - `delivery_rate` (**float**): The rate charged per kilowatt-hour for the delivery of electricity from the power generation point to your location.
  
  - `supply_rate` (**float**): The rate charged per kilowatt-hour for the actual electricity consumed. This may vary based on the supplier.
  
  - `interval_start` (**str**): The starting date of the billing cycle, formatted as MM/DD/YYYY.
  
  - `interval_end` (**str**): The ending date of the billing cycle, formatted as MM/DD/YYYY.
  
  - `kwh_delivered` (**int**): The total amount of electricity consumed during the billing cycle, measured in kilowatt-hours.
  
  - `pdf_file_name` (**str**): The name of the PDF file from which the billing information was extracted. Useful for tracking the source of data.
  
  - `account_number` (**int**): A unique identifier assigned by Central Maine Power for the customer's account. Used for all billing and service interactions.

### `dim_datetimes`

A detailed dimensional table that contains the breakdown of timestamps into individual date and time components, along with a classification of each time into a specific period of the day such as 'Off-peak', 'Mid-peak', or 'On-peak'. This table is key for time series analysis and enables efficient filtering and aggregation based on time attributes in data analysis workflows.

**Source**: Derived from `meter_usage` DataFrame  
**Location**: `./data/model/dim_datetimes` 

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a surrogate key.
  
  - `timestamp` (**datetime**): The exact date and time the measurement was taken, precise up to minutes.

  - `increment` (**int**): The minute component of the timestamp, indicating the 15-minute interval.
  
  - `hour` (**int**): The hour component of the timestamp, represented in a 24-hour format.
  
  - `date` (**date**): The date component of the timestamp in YYYY-MM-DD format.
  
  - `week` (**int**): The week number of the year when the timestamp occurs, according to ISO standards.
  
  - `week_in_year` (**int**): Duplicate of `week` for legacy support; represents the ISO week number within the year.
  
  - `month` (**int**): The month number extracted from the timestamp.
  
  - `month_name` (**str**): The full name of the month extracted from the timestamp.
  
  - `quarter` (**int**): The quarter of the year to which the timestamp belongs.
  
  - `year` (**int**): The year component extracted from the timestamp.
  
  - `period` (**str**): A categorical label defining the time period of the day based on the hour, used for analysis of peak and off-peak hours.

### `dim_accounts`

A centralized dimensional table that aggregates meter-specific information, like service points, meter IDs, and location details. It merges dimensions from various curated sources into a single table, enabling easier categorization and making the data more accessible for analysis.

**Source**: Derived from the `meter_usage` and `locations` DataFrames  
**Location**: `./data/model/dim_meters` 

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a primary key.

  - `meter_id` (**str**): The unique identifier for the meter that records energy consumption data.

  - `service_point_id` (**int**): A unique identifier for the physical location where energy consumption is measured.
  
  - `account_number` (**str**): The unique identifier for each customer account, which can serve as a foreign key to other curated data.
  
  - `street` (**str**): The street address associated with the service point, providing a granular location detail.
  
  - `label` (**str**): A descriptive label for the location, often used for easier identification or categorization of the service area.

### `dim_suppliers`

A concise dimensional table that stores information about energy suppliers and their associated average supply rates. This table assists with financial analysis and comparisons between supplier costs.

**Source**: Derived from the `cmp_bills` DataFrame  
**Location**: `./data/model/dim_suppliers` 

**Schema**:

  - `id` (**int**): A unique identifier assigned sequentially starting at 1 for each supplier, serving as a primary key.
  
  - `supplier` (**str**): The name of the energy supplier, which can be used as a foreign key to join with transactional data related to billing and consumption.
  
  - `supply_rate` (**float**): The average rate at which the supplier charges for energy, vital for cost analysis and supplier comparison.


## Curation

This section comprises functions that transform raw data files into structured and query-optimized formats. This includes converting raw CSVs into partitioned Parquet files and extracting relevant data from PDFs.

### `curate_meter_usage`

Processes all CSV files from the provided directory, integrates the specified schema, and subsequently consolidates the data into a partitioned `.parquet` file in the designated output directory. This conversion and curation process is optimized with `snappy` compression for efficiency.

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

### `curate_cmp_bills`

Processes all CSV files from the provided directory (*likely one file only with this workflow*) and consolidates the data into a partitioned `.parquet` file in the designated output directory. This conversion and curation process is optimized with `snappy` compression for efficiency.

**Signature** 
```python
def curate_meter_usage(raw           : str, 
                       curated       : str, 
                       partition_col : list):
```

**Parameters**

- **`raw`**: Path to the directory containing raw `cmp/bills` CSV files.

- **`curated`** : Directory where the consolidated `.parquet` files will be saved.

- **`partition_col`**: Columns by which the .parquet files will be partitioned.
 
### `scrape_cmp_bills`
 
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
    - **Scenario**: Some bills may have more than one billing interval, often crossing fiscal quarters.  
    - **Action**: Add an additional row in the CSV with the same `account_number`, `amount_due`, and `pdf_file_name`, but with differing values for other fields.  
        - **Special Case**: If the bill presents a single `kwh_delivered` value for multiple intervals, allocate a percentage of this to each interval based on the percentage of total `service_charge` paid.  
    - **Example**: See [`30010320353/703001515406_bill.pdf`](../data/cmp/raw/bills/30010320353/703001515406_bill.pdf) and [`30010320353/702001847715_bill.pdf`](../data/cmp/raw/bills/30010320353/702001847715_bill.pdf).

4. **Addressing Missing 'Delivery Service Rate'**:  
    - **Scenario**: Some bills do not include a `delivery_service_rate`.  
    - **Action**: Leave this field as NULL, with the understanding that the delivery was covered by previous `Banked Generation`.  
    - **Example**: See [`30010320353/705001871139_bill.pdf`](../data/cmp/raw/bills/30010320353/705001871139_bill.pdf).

5. **External Electricity Supply**:  
    - **Scenario**: In cases where electricity is supplied by an external provider.  
    - **Action**: Manually add the `supplier` and `supply_rate`.
    - **Example**: See [`30010320353/701001868909_bill.pdf`](../data/cmp/raw/bills/30010320353/701001868909_bill.pdf).

After these manual interventions are performed, the curated CSV file is ready for conversion into Parquet format for further data processing.

## Modeling

This section comprises functions that transform DataFrames into a structured, denormalized data model optimized for analytical queries and data visualization. It includes the generation of dimensional tables and the enhancement of timestamp data to facilitate intuitive querying.

### `create_dim_datetimes`

Generates a datetime dimension table, which is a key component in time series analysis and reporting. It enriches the dataset by breaking down timestamps into more granular and useful components, facilitating more sophisticated temporal queries and analyses.

**Methodology**

1. Extract and sort unique timestamps from `meter_usage['interval_end_datetime']`.
2. Decompose timestamps into individual time components.
3. Categorize timestamps into time periods based on the hour of the day.
4. Assign a unique identifier `id` to each timestamp.
5. Persist the resulting dataframe as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file saved in the specified `model` directory containing the datetime dimension table.

**Example Output**

| id      | timestamp           | increment | hour | date       | week | week_in_year | month | month_name | quarter | year | period                                 |
|---------|---------------------|-----------|------|------------|------|--------------|-------|------------|---------|------|----------------------------------------|
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

### `create_dim_meters`

Creates a meters dimension table, which centralizes the account information, linking service points, meter IDs, and location details. This table simplifies the complexity of account management and enables more efficient meter-related queries and analyses. This table originally was curated for accounts, but through the discovery process, we learned that it's possible for a given account to have multiple meters and service points.

**Methodology**

1. Read the `meter_usage` and `locations` DataFrames.
2. Extract and join relevant columns based on `account_number`.
3. Assign a unique identifier `id` to each account entry.
4. Persist the resulting dataframe as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file saved in the specified `model` directory containing the accounts dimension table.

**Example Output**

| id | meter_id   | service_point_id | account_number | street               | label          |
|----|------------|------------------|----------------|----------------------|----------------|
| 1  | L108605388 | 2300822246       | 30010320353    | 115 FOX ST UNIT 115  | Fox Street     |
| 2  | L108558642 | 2300822209       | 30010320361    | 115 FOX ST UNIT 103  | Fox Street     |
| 3  | L108557737 | 2300910019       | 30010601281    | 111 FOX ST UNIT 2    | Fox Street     |
|... | ...        | ...              | ...            | ...                  | ...            |
| 8  | L108607371 | 2300588897       | 35012790198    | 1 INDUSTRIAL WAY U10 | Industrial Way |


### `create_dim_suppliers`

Creates a suppliers dimension table, which encapsulates the supplier information and the average supply rates offered. This table is essential for understanding the opportunities Austin Street can move away from in banking more solar credit as their own supply.

**Methodology**

1. Read the `cmp_bills` DataFrame.
2. Group by `supplier` and calculate the average `supply_rate`.
3. Assign a unique identifier `id` to each supplier entry.
4. Persist the resulting dataframe as a `.parquet` file with `snappy` compression.

**Returns**

A `.parquet` file saved in the specified `model` directory containing the suppliers dimension table.

**Example Output**

| id    | supplier             | supply_rate |
|-------|----------------------|-------------|
| 1     | Constellation        | 0.11780     |
| 2     | Mega Energy of Maine | 0.06840     |
| 3     | Standard Offer       | 0.17631     |
| 4     | Town Square Energy   | 0.06840     |
