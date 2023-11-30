<!-- omit in toc -->
# Data Dictionary for Electric Brew

This document provides detailed descriptions of all data elements within our project, including tables, fields, and their respective data types. For a more comprehensive understanding, it is recommended to view this in conjunction with our [Entity-Relationship Diagram (ERD)](/data/sql/README.md). For a technical understanding of how the underlying data for these tables was handled, take a look at our documentation on the [Data Pipeline Functions](/src/README.md) in this project.

<!-- omit in toc -->
- [`meter_usage`](#meter_usage)
- [`locations`](#locations)
- [`cmp_bills`](#cmp_bills)
- [`ampion_bills`](#ampion_bills)
- [`dim_datetimes`](#dim_datetimes)
- [`dim_meters`](#dim_meters)
- [`dim_bills`](#dim_bills)
- [`fct_electric_brew`](#fct_electric_brew)

## `meter_usage`

A repository for meter-level electrical consumption data from Central Maine Power (CMP) in 15-minute intervals. Used in analyses of electricity usage patterns, billing, and location-related insights. The DataFrame is partitioned by `account_number`, enabling quick data retrieval for individual accounts. 

**Source**: Central Maine Power (CMP)  
**Location**: `./data/cmp/curated/meter_usage`  
**Partitioning**: `account_number`  

**Schema**:

  - `service_point_id` (**int**): A unique identifier for the point where the electrical service is provided, often tied to a specific location or customer.
  
  - `meter_id` (**str**): Identifier for the electrical meter installed at the service point. It records the amount of electricity consumed.
  
  - `interval_end_datetime` (**str**): Timestamp marking the end of the meter reading interval, typically indicating when the meter was read.
  
  - `meter_channel` (**int**): The channel number on the electrical meter. Meters with multiple channels can record different types of data.
  
  - `kwh` (**float**): Kilowatt-hours recorded by the meter during the interval, representing the unit of electricity consumed.

  - `account_number` (**int**): A unique identifier assigned by Central Maine Power for the customer's account. Used for all billing and service interactions.

## `locations`

A DataFrame that contains location-based information for CMP accounts, linking street addresses to account numbers. It is essential for correlating energy consumption with specific locations and their equipment.

**Source**: Manual Entry
**Location**: `./data/cmp/curated/locations`  
**Partitioning**: `account_number`  

**Schema**:

  - `street` (**str**): The street address associated with the CMP account, detailing the exact location.
  
  - `label` (**str**): A simplified or common name label for the location, which may be used for easier reference.
  
  - `account_number` (**int**): A unique identifier assigned by Central Maine Power for the customer's account, linking the location to the specific account for billing and service interactions.

## `cmp_bills`

A consolidated view of billing information from various suppliers for Central Maine Power (CMP) accounts. The DataFrame is partitioned by `account_number`, making it easy to retrieve data for specific accounts quickly.

**Source**: Central Maine Power (CMP)  
**Location**: `./data/cmp/curated/bills`  
**Partitioning**: `account_number`  

**Schema**:

- `invoice_number` (**str**): The unique identifier for each invoice, representing a specific billing period. Useful for tracking the source of data.

- `amount_due` (**float**): The total monetary amount due for the billing period as stated on the bill, encompassing all charges, including energy consumption, delivery services, taxes, and other fees.

- `delivery_tax` (**float**): The tax amount levied on the delivery component of the electricity service by the state of Maine.

- `interval_start` (**str**): The start date of the billing cycle, formatted as YYYY-MM-DD. 

- `interval_end` (**str**): The end date of the billing cycle, formatted as YYYY-MM-DD. 

- `service_charge` (**float**): A fixed fee assessed for the delivery of electricity. 
  This charge covers the maintenance and use of the electrical grid infrastructure.

- `kwh_delivered` (**int**): The total amount of electricity, in kilowatt-hours, delivered to the customer during the billing cycle.

- `delivery_charge` (**float**): The charge for the delivery of electricity measured by `kwh_delivered`. These two facts are used to calculate `delivery_rate`.

- `supplier` (**str**): The name of the company supplying the electricity. This can vary if the customer has chosen a supplier other than CMP.

- `kwh_supplied` (**int**): The total amount of electricity, in kilowatt-hours, the `supplier` was responsible for supplying during the billing cycle.

- `supply_charge` (**float**): The charge for the electricity supplied measured by `kwh_supplied`. These two facts are used to calculate `supply_rate`.

- `supply_tax` (**float**): The tax amount levied on the supply component of the electricity service by the state of Maine.

- `account_number` (**str**): A unique identifier assigned by Central Maine Power for the customer's account. It is used for both billing and service interactions and is a consistent key within `meter_usage`, `cmp_bills`, and `ampion_bills`.

## `ampion_bills`

A consolidated view of billing data from Ampion, structured to provide easy access to detailed information about energy usage and pricing for each account, based on which tier Austin Street was opted into at the time of the bill. This DataFrame is crucial for tracking the full cost of delivery over time.

**Source**: Ampion  
**Location**: `./data/ampion/curated`  
**Partitioning**: `account_number`  

**Schema**:

- `invoice_number` (**str**): The unique identifier for each invoice, representing a specific billing period. Useful for tracking the source of data.

- `supplier` (**str**): The name of the energy supplier, which can be used as a foreign key to join with transactional data related to billing and consumption.

- `interval_start` (**str**): The start date of the billing cycle, formatted as YYYY-MM-DD. Indicates the beginning of the period for which the electricity usage is being billed.

- `interval_end` (**str**): The end date of the billing cycle, formatted as YYYY-MM-DD. Marks the closure of the period for which the electricity usage is being billed.

- `kwh` (**int**): The total amount of electricity supplied by Ampion during the billing cycle, measured in kilowatt-hours (kWh).

- `bill_credits` (**float**): The total monetary value of renewable energy credits allocated to the account, reflecting the benefits of participating in renewable energy programs.

- `price` (**float**): The adjusted price charged for energy supply and consumption, after applying renewable energy bill credits, representing the final cost to the customer.

- `account_number` (**str**): A unique identifier originally assigned by CMP for each customer's account, facilitating billing and service interactions. It is a consistent key within `meter_usage`, `cmp_bills`, and `ampion_bills`.

## `dim_datetimes`

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
  
  - `week_start` (**datetime**): The preceding Monday of the week in which the timestamp occurs.
  
  - `month` (**int**): The month number extracted from the timestamp.
  
  - `month_name` (**str**): The full name of the month extracted from the timestamp.

  - `month_start` (**datetime**): The starting date of the month in which the timestamp occurs.
  
  - `quarter` (**int**): The quarter of the year to which the timestamp belongs.
  
  - `year` (**int**): The year component extracted from the timestamp.
  
  - `period` (**str**): A categorical label defining the time period of the day based on the hour, used for analysis of peak and off-peak hours.

## `dim_meters`

A centralized dimensional table that aggregates detailed information specific to electricity meters, including service points, meter IDs, and associated location details. This table merges dimensions from various curated sources into a single comprehensive table, facilitating easier analysis and categorization of energy consumption data.

**Source**: Derived from the `meter_usage` and `locations` DataFrames  
**Location**: `./data/modeled/dim_meters` 

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a primary key.

  - `meter_id` (**str**): The unique identifier for the electricity meter that records energy consumption data.

  - `service_point_id` (**int**): A unique identifier for the physical location where energy consumption is measured by the meter.

  - `account_number` (**str**): A unique identifier originally assigned by CMP for each customer's account, facilitating billing and service interactions. It is a consistent key within `meter_usage`, `cmp_bills`, and `ampion_bills`.

  - `street` (**str**): The street address associated with the service point, providing detailed location information.

  - `label` (**str**): A descriptive name for the location, used for easier categorization and reporting of the service area.

  - `operational_area` (**str**): A succinct label for the series of operations that a particular location/meter is responsible for.

## `dim_bills`

A comprehensive dimensional table that combines detailed billing information from both Central Maine Power (CMP) and Ampion. This table is pivotal for analyzing overall energy consumption, costs, and understanding the nuances of billing from different energy suppliers. It merges the structured data from CMP's diverse suppliers with the nuanced billing details of Ampion, including renewable energy credits and adjusted pricing.

**Source**: Derived from `cmp_bills` and `ampion_bills` DataFrames  
**Location**: `./data/modeled/dim_bills`  

**Schema**:

  - `id` (**int**): A unique identifier starting at 1 for each row in the table, serving as a primary key.

  - `invoice_number` (**str**): The unique identifier for each invoice, encapsulating data for specific billing periods from both CMP and Ampion, crucial for tracking and analysis.

  - `account_number` (**str**): A unique identifier originally assigned by CMP for each customer's account, facilitating billing and service interactions. It is a consistent key within `meter_usage`, `cmp_bills`, and `ampion_bills`.

  - `supplier` (**str**): The name of the energy supplier, reflecting either CMP's third-party suppliers or Ampion's renewable energy provision, essential for supplier-based comparisons and analysis.

  - `kwh_delivered` (**float**): The total kilowatt-hours of energy delivered, combining CMP's electricity consumption metrics with Ampion's supplied and delivered kWh.

  - `service_charge` (**float**): The service charge applied, derived from CMP's volume-based fees, indicative of the fixed costs associated with energy delivery.

  - `taxes` (**float**): The sum of both the `delivery_tax` and `supply_tax` from CMP's billing structure, representing the total tax obligation to the state of Maine on each bill.

  - `delivery_rate` (**float**): The rate charged per kilowatt-hour for the delivery of electricity, a crucial metric for understanding delivery cost structures across CMP suppliers.

  - `supply_rate` (**float**): The rate charged per kilowatt-hour for the energy supply, incorporating both CMP's supplier rates and Ampion's adjusted pricing, vital for cost analysis.

  - `source` (**str**): The origin of the billing data ('CMP' or 'Ampion').

  - `billing_interval` (**list[date]**): A list of dates representing the entire billing cycle, from the start to the end date, providing a detailed view of the billing period for each record.

## `fct_electric_brew`

The `fct_electric_brew` table is the central fact table that consolidates detailed records of electricity usage and associated costs for each account within specific billing intervals. This comprehensive dataset merges and transforms detailed meter readings, billing information, and rate schedules to provide insights into electricity consumption, cost structures, and temporal usage patterns. It is pivotal for profitability analysis, cost allocation, and understanding the dynamics of electricity usage and charges.

**Source**: This table is synthesized from the `meter_usage`, `cmp_bills`, `ampion_bills`, `dim_meters`, `dim_datetimes`, and `dim_bills` DataFrames. The synthesis involves expanding billing intervals to a daily granularity, merging with meter and datetime dimensions, allocating service charges based on usage, and aggregating costs.

**Location**: `./data/modeled/fct_electric_brew`

**Schema**:

  - `id` (**int**): The primary key of the table, assigned sequentially starting from 1, uniquely identifying each record in the dataset.

  - `dim_datetimes_id` (**int**): References the `dim_datetimes` table, providing a reference to the exact date and time corresponding to each meter reading, essential for analyzing usage patterns over time.

  - `dim_meters_id` (**int**): References the `dim_meters` table, indicating the specific meter through which electricity consumption data was recorded, crucial for understanding the geographical and physical source of consumption data.

  - `dim_bills_id` (**int**): References the `dim_bills` table, indicating the source of billing data (CMP or Ampion), which is vital for differentiating the billing methodologies and cost calculations.

  - `kwh` (**float**): Represents the total electricity consumed in kilowatt-hours during the billing interval, serving as the basis for cost calculations and usage analysis.

  - `delivery_cost` (**float**): The calculated cost associated with the delivery of electricity from CMP, derived from the used kWh and the delivery rate.

  - `service_cost` (**float**): The portion of the total cost allocated for service charges, proportionally distributed based on kWh usage within the billing interval.

  - `supply_cost` (**float**): The cost for the electricity supply itself, computed from the used kWh and respective supply rates, which varies based on the source of billing and the type of supply contract.

  - `tax_cost` (**float**): The portion of the total Maine taxes for delivery and supply, calculated proportionally based on the kWh usage within the billing interval.

  - `total_cost` (**float**): The aggregate cost incurred, encompassing delivery, service, supply, and tax costs, providing a comprehensive view of the financial impact of electricity consumption for each account on each meter reading.

  - `account_number` (**str**): A unique identifier originally assigned by CMP for each customer's account, facilitating billing and service interactions. It is a consistent key within `meter_usage`, `cmp_bills`, and `ampion_bills`.