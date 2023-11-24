from curation import *
from modeling import *
from runtime  import connect_to_db

'''
ETL (Extract, Transform, Load) Script for Electric Brew Project

This script facilitates the ETL process for the Electric Brew project. It encompasses a series of operations starting 
from raw data extraction, data curation and transformation into Parquet format, through to modeling and loading the data into a database 
for analysis and reporting. It leverages functions defined in the `curation`, `modeling`, and `runtime` modules.

Workflow Summary:
1. Raw Data Scraping: Extracts comprehensive billing and usage data from CMP and Ampion for granular energy analysis.
2. Data Curation: Cleans, filters, and structures raw data into a cohesive, query-optimized format for deeper insights.
3. Data Modeling: Creates dimensional and fact tables to facilitate multifaceted, efficient data analysis and reporting.
4. Database Integration: Initializes a DuckDB connection and creates pointer views for direct, efficient SQL querying.

Functions:
    - Raw Data Extraction
        - scrape_cmp_bills(): Retrieves raw billing data from Central Maine Power (CMP). This includes details like 
          billing periods, amounts, and associated account information.
        - scrape_ampion_bills(): Gathers raw billing data from Ampion, focusing on renewable energy credits and 
          related billing details.

    - Data Curation
        - write_results(load_data_files()): A two-step process that first loads raw data files from specified 
          paths and then writes this data into a curated format. The curation process includes filtering, cleaning, 
          and structuring data into a format that's more conducive to analysis. It's applied to various datasets like 
          meter usage, locations, and billing information from CMP and Ampion.

    - Data Modeling
        - model_dim_datetimes(): Constructs a dimensional table for datetime information. This model is essential 
          for time-based analyses, enabling more straightforward querying and reporting on temporal aspects like 
          billing cycles, usage patterns, and historical trends.
        - model_dim_meters(): Develops a dimensional model for meter-related data, encompassing aspects like meter 
          IDs, service points, and account numbers. This model aids in analyzing electricity usage at a granular 
          meter-level scale.
        - model_dim_bills(): Creates a dimensional model for billing data. This includes structuring and organizing 
          various billing attributes for easier access and analysis, particularly useful for financial and usage 
          cost analyses.
        - model_fct_electric_brew(): Builds the central fact table for the Electric Brew project. This table 
          integrates key metrics and dimensions from other models, providing a comprehensive view for complex 
          analytical queries and decision-making processes at the `meter_usage` grain, in which each record is a kWh
          reading from one of Austin Street's meters.

These functions and sections collectively form the backbone of the Electric Brew project's data pipeline, ensuring data 
is accurately extracted, transformed, and loaded for effective analysis and reporting.

'''

# RAW DATA EXTRACTION (`/raw/parquet/`)

scrape_cmp_bills()
scrape_ampion_bills()


# DATA CURATION (`/curated/`)

# CMP meter usage data
write_results(
    load_data_files(path="./data/cmp/raw/meter_usage",
                    cols=["account_number", "service_point_id", "meter_id", "interval_end_datetime", "meter_channel", "kwh"]),
                    dest="./data/cmp/curated/meter_usage")

# CMP location data
write_results(
    load_data_files(path="./data/cmp/raw/locations"),
                    dest="./data/cmp/curated/locations")

# CMP billing data
write_results(
    load_data_files(path="./data/cmp/raw/bills/parquet",
                    type='parquet'),
                    dest="./data/cmp/curated/bills")

# Ampion billing data
write_results(
    load_data_files(path="./data/ampion/raw/parquet", 
                    type='parquet'), 
                    dest="./data/ampion/curated")


# DATA MODELING (`/modeled/`)

model_dim_datetimes()
model_dim_meters()
model_dim_bills()
model_fct_electric_brew()


# DATABASE INTEGRATION (`/sql/`)

connect_to_db()
