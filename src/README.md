<!-- omit in toc -->
# Utility Functions

The `/src/` directory contains a `utils` module full of scripts that support the main functionalities of this project, aligning with conventional structuring in data science and software development projects. This document provides a brief overview of each function and its purpose, signature, and expected usage.

> **Note**: To facilitate smooth development and execution, it's recommended to run all commands out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** is set to point directly to the `/src/` directory within this Conda environment. This allows you to easily import the `utils` module and its DataFrames and functions from any script within `/src/`.

<!-- omit in toc -->
## Table of Contents
- [Runtime](#runtime)
  - [`set_plot_params`](#set_plot_params)
  - [`read_data`](#read_data)
- [DataFrames](#dataframes)
  - [`meter_usage`](#meter_usage)
  - [`locations`](#locations)
  - [`cmp_bills`](#cmp_bills)
  - [`ampion_bills`](#ampion_bills)
  - [`dim_datetimes`](#dim_datetimes)
  - [`dim_accounts`](#dim_accounts)
  - [`dim_bills`](#dim_bills)
  - [`fct_electric_brew`](#fct_electric_brew)
- [Curation](#curation)
  - [`curate_meter_usage`](#curate_meter_usage)
    - [**Regular Expressions**](#regular-expressions)
    - [**Manual Interventions**](#manual-interventions)
  - [`scrape_ampion_bills`](#scrape_ampion_bills)
    - [**Regular Expressions**](#regular-expressions-1)
    - [**Manual Interventions**](#manual-interventions-1)
- [Modeling](#modeling)
  - [`create_dim_datetimes`](#create_dim_datetimes)
  - [`create_dim_meters`](#create_dim_meters)
  - [`create_dim_bills`](#create_dim_bills)
  - [`create_fct_electric_brew`](#create_fct_electric_brew)
  - [`create_electric_brew_db`](#create_electric_brew_db)
    - [Key Constraints](#key-constraints)


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

  - `invoice_number` (**str**): The unique identifier for each invoice, representing a specific billing period. Useful for tracking the source of data.
  - 
  - `supplier` (**str**): The electricity supplier for the billing period. This is often a third-party energy supplier, though some billing periods use "Banked Generation" and credits from prebious supplier purchases.
  
  - `amount_due` (**float**): The total monetary amount due for the billing period. This includes all charges, fees, and taxes.
  
  - `service_charge` (**float**): A volume-based fee charged for delivery service through the electrical grid by CMP.
  
  - `delivery_rate` (**float**): The rate charged per kilowatt-hour for the delivery of electricity from the power generation point to your location.
  
  - `supply_rate` (**float**): The rate charged per kilowatt-hour for the actual electricity consumed. This may vary based on the supplier.
  
  - `interval_start` (**str**): The starting date of the billing cycle, formatted as MM/DD/YYYY.
  
  - `interval_end` (**str**): The ending date of the billing cycle, formatted as MM/DD/YYYY.
  
  - `kwh_delivered` (**int**): The total amount of electricity consumed during the billing cycle, measured in kilowatt-hours.
  
  - `account_number` (**int**): A unique identifier assigned by Central Maine Power for the customer's account, facilitating billing and service interactions.

### `ampion_bills`

A consolidated view of billing data from Ampion, structured to provide easy access to detailed information about energy usage and pricing for each account, based on which tier Austin Street was opted into at the time of the bill. This DataFrame is crucial for tracking the full cost of delivery over time.

**Source**: Ampion  
**Location**: `./data/ampion/curated/bills`  
**Partitioning**: `account_number`  

**Schema**:

- `invoice_number` (**str**): The unique identifier for each invoice, representing a specific billing period. Useful for tracking the source of data.

- `supplier` (**str**): The name of the energy supplier, which can be used as a foreign key to join with transactional data related to billing and consumption.

- `interval_start` (**str**): The start date of the billing cycle, formatted as YYYY-MM-DD.

- `interval_end` (**str**): The end date of the billing cycle, formatted as YYYY-MM-DD.
  
- `kwh` (**int**): The total amount of electricity supplied by Ampion during the billing cycle, measured in kilowatt-hours (kWh).
  
- `bill_credits` (**float**): The total monetary value of renewable energy credits allocated to the account, reflecting the benefits of participating in renewable energy programs.
  
- `price` (**float**): The adjusted price charged for energy supply and consumption, after applying renewable energy bill credits, representing the final cost to the customer.
  
- `account_number` (**str**): A unique identifier originally assigned by CMP for each customer's account, facilitating billing and service interactions.
  

### `dim_datetimes`

A detailed dimensional table that contains the breakdown of timestamps into individual date and time components, along with a classification of each time into a specific period of the day such as 'Off-peak', 'Mid-peak', or 'On-peak'. This table is key for time series analysis and enables efficient filtering and aggregation based on time attributes in data analysis workflows.

**Source**: Derived from `meter_usage` DataFrame  
**Location**: `./data/modeled/dim_datetimes` 

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a primary key.
  
  - `timestamp` (**datetime**): The exact date and time the measurement was taken, precise up to minutes.

  - `increment` (**int**): The minute component of the timestamp, indicating the 15-minute interval.
  
  - `hour` (**int**): The hour component of the timestamp, represented in a 24-hour format.
  
  - `date` (**date**): The date of the timestamp, normalized to midnight of that day.
  
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
**Location**: `./data/modeled/dim_meters` 

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a primary key.

  - `meter_id` (**str**): The unique identifier for the meter that records energy consumption data.

  - `service_point_id` (**int**): A unique identifier for the physical location where energy consumption is measured.
  
  - `account_number` (**str**): The unique identifier for each customer account, which can serve as a foreign key to other curated data.
  
  - `street` (**str**): The street address associated with the service point, providing a granular location detail.
  
  - `label` (**str**): A descriptive label for the location, often used for easier identification or categorization of the service area.

### `dim_bills`

A comprehensive dimensional table that combines detailed billing information from both Central Maine Power (CMP) and Ampion. This table is pivotal for analyzing overall energy consumption, costs, and understanding the nuances of billing from different energy suppliers. It merges the structured data from CMP's diverse suppliers with the nuanced billing details of Ampion, including renewable energy credits and adjusted pricing.

**Source**: Derived from `cmp_bills` and `ampion_bills` DataFrames  
**Location**: `./data/modeled/dim_bills`  

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a primary key.

  - `invoice_number` (**str**): The unique identifier for each invoice, encapsulating data for specific billing periods from both CMP and Ampion, crucial for tracking and analysis.

  - `account_number` (**str**): The identifier assigned by CMP, used consistently across both CMP and Ampion, enabling seamless integration and comparison of billing data.

  - `supplier` (**str**): The name of the energy supplier, reflecting either CMP's third-party suppliers or Ampion's renewable energy provision, essential for supplier-based comparisons and analysis.

  - `kwh_delivered` (**float**): The total kilowatt-hours of energy delivered, combining CMP's electricity consumption metrics with Ampion's supplied and delivered kWh.

  - `service_charge` (**float**): The service charge applied, derived from CMP's volume-based fees, indicative of the fixed costs associated with energy delivery.

  - `delivery_rate` (**float**): The rate charged per kilowatt-hour for the delivery of electricity, a crucial metric for understanding delivery cost structures across CMP suppliers.

  - `supply_rate` (**float**): The rate charged per kilowatt-hour for the energy supply, incorporating both CMP's supplier rates and Ampion's adjusted pricing, vital for cost analysis.

  - `source` (**str**): The origin of the billing data ('CMP' or 'Ampion').

  - `billing_interval` (**list[date]**): A list of dates representing the entire billing cycle, from the start to the end date, providing a detailed view of the billing period for each record.

### `fct_electric_brew`

The `fct_electric_brew` table serves as the primary fact table, capturing detailed records of electricity usage and the associated costs for each of Austin Street's accounts within specified billing intervals. It provides a comprehensive view of electric consumption, delivery charges, and financial metrics that are necessary for profitability analysis and peak hour usage. It is constructed by integrating and transforming data from various sources, including meter readings, billing details, and rate schedules.

**Source**: Synthesized from `meter_usage`, `cmp_bills`, `ampion_bills`, `dim_meters`, `dim_datetimes`, and `dim_bills` DataFrames through a series of complex transformations that involve data expansion, merging, mapping of usage metrics, and summarization of costs.

**Location**: `./data/modeled/fct_electric_brew`

**Schema**:

  - `id` (**int**): A unique identifier for each record, serving as a primary key and generated by sequential numbering starting at 1.
  
  - `dim_datetimes_id` (**int**): A reference key to the `dim_datetimes` table, linking to the specific date and time the data pertains to.
  
  - `dim_meters_id` (**int**): A reference key to the `dim_meters` table, indicating the meter through which the electricity usage data was recorded.
  
  - `dim_bills_id` (**int**): A reference key to the `dim_bills` table, denoting which source the information about the billing interval came from.
  
  - `account_number` (**str**): The identifier for the customer's account, used to correlate the record with a specific location and set of meters in the dataset.
  
  - `kwh` (**float**): The quantity of electricity in kilowatt-hours that has been consumed in the specified time frame.
  
  - `cost` (**float**): The total cost associated with delivering electricity to the account, calculated as the sum of the product of the used kilowatt-hours and the sum of delivery and supply rates, plus the allocated service charge.


## Curation

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

- **`raw`**: Path to the directory containing raw `meter-usage` CSV files.

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

#### **Manual Interventions**

After the automated scraping process is completed by `scrape_ampion_bills`, one additional intervention must be taken in order for data completeness.

1. **Add Miscellaneous Adjustments to `misc_adjustments.csv`**:  
    - **Initial State**: Some bills have an additional non-standard section that explains in text why the bill has an additional charge for a given account. 
    - **Action**: Manually add a row to `misc_adjustments.csv` that replicates the existing schema from the other CSVs in the directory.
    - **Example**: See [`202310.pdf`](../data/ampion/raw/bills/pdf/202310.pdf).


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

### `create_dim_meters`

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


### `create_dim_bills`

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

### `create_fct_electric_brew`

This function constructs the `fct_electric_brew` fact table, which is central to the analytics model for Austin Street Brewery's electricity consumption and cost analysis. It records detailed electric usage and the associated charges for each customer account at specific time intervals.

**Methodology**

1. The billing data in both `cmp_bills` and `ampion_bills` is expanded to a daily granularity to align with the usage data from `meter_usage`, ensuring that daily charges can be accurately associated with the usage data.
2. An intermediary DataFrame is curated by merging cleaned and transformed `meter_usage` data with dimension tables (`dim_meters`, `dim_datetimes`, and an exploded version of `dim_bills`). This provides a comprehensive view of the usage and billing information.
3. Cumulative and usage-based metrics such as `allocated_service_charge`, `delivered_kwh_left`, and `delivered_kwh_used` are calculated to provide insights into the usage patterns and remaining delivery capacities.
4. The `total_cost_of_delivery` is computed by summing the delivery and supply rates, along with the allocated service charge, to determine the total cost associated with the electric delivery for each usage entry.
5. A unique identifier `id` is assigned to each row, providing a primary key for database integrity and indexation.
6. The final DataFrame is saved as a `.parquet` file in the specified `modeled` directory with `snappy` compression and partitioned by `account_number` to optimize storage and query performance.

**Returns**

A `.parquet` file saved in the specified `modeled` directory containing the central fact table.

**Example Output**

| id     | dim_datetimes_id | dim_meters_id | dim_bills_id | kwh   | cost    | account_number |
|--------|------------------|---------------|------------------|-------|---------|----------------|
| 42149  | 42141            | 1             | 2.0              | 0.653 | 0.003094 | 30010320353    |
| 42150  | 42142            | 1             | 2.0              | 0.511 | 0.002421 | 30010320353    |
| 42151  | 42143            | 1             | 2.0              | 0.745 | 0.003530 | 30010320353    |
| 42152  | 42144            | 1             | 2.0              | 0.517 | 0.002450 | 30010320353    |
| 42153  | 42145            | 1             | 2.0              | 0.731 | 0.003464 | 30010320353    |
| ...    | ...              | ...           | ...              | ...   | ...     | ...            |
| 498356 | 96737            | 8             | 3.0              | 4.318 | 1.133586 | 35012790198    |
| 498357 | 96741            | 8             | 3.0              | 4.403 | 1.155901 | 35012790198    |
| 498358 | 96745            | 8             | 3.0              | 3.965 | 1.040915 | 35012790198    |
| 498359 | 96749            | 8             | 3.0              | 4.156 | 1.091057 | 35012790198    |
| 498360 | 96753            | 8             | 3.0              | 3.994 | 1.048528 | 35012790198    |

### `create_electric_brew_db`

This function initializes and populates the `electric_brew` SQLite database for the Electric Brew project. It creates several tables with predefined schemas, including primary keys and foreign key relationships, to store electric consumption and billing data.

Each `/curated/` or `/modeled/` DataFrame, outlined above in [DataFrames](#dataframes), has a corresponding table in this database.

#### Key Constraints

1. `dim_datetimes`
   - **Primary Key**: `id`

2. `dim_meters`
   - **Primary Key**: `id`

3. `dim_suppliers`
   - **Primary Key**: `id`

4. `fct_electric_brew`
   - **Primary Key**: `id`
   - **Foreign Keys**:
     - `dim_datetimes_id` references `dim_datetimes(id)`
     - `dim_meters_id` references `dim_meters(id)`
     - `dim_bills_id` references `dim_bills(id)`

**Methodology**

The database is created and populated by:
1. Establishing a connection to the specified SQLite database file.
2. Defining tables with their respective columns, types, and constraints.
3. Creating all tables in the database.
4. Inserting data from provided Pandas DataFrames into the corresponding tables.

**ERD Location**

A comprehensive Entity-Relationship Diagram (ERD) of the database can be found in the `sql` directory's [README](../data/sql/README.md). This ERD provides a visual representation of the tables, their relationships, and constraints.