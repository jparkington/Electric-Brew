<!-- omit in toc -->
# Explortatory Data Analysis  

<!-- omit in toc -->
## Table of Contents

- [Introduction](#introduction)
- [Installation \& Usage](#installation--usage)
  - [Setting Up the Environment](#setting-up-the-environment)
- [General Information](#general-information)

## Introduction

The following is an exploratory data analysis of energy usage data for Austin Street Brewery Company. The raw data has been procured directly from Central Maine Power (CMP), which is Austin Street's principal energy supplier. 

## Installation & Usage

This project utilizes [**Conda**](https://docs.conda.io/en/latest/) for managing dependencies and environments. Conda enables the creation of isolated environments that can house specific versions of packages, making it easier to manage complex projects. It's particularly well-suited for Python-based data science projects, ensuring that all dependencies are compatible and can be easily installed or removed.

### Setting Up the Environment

For the shortest path to getting up and running, you can simply run the command below, which will create the Conda environment and run commands for each EDA script generated for this project:

```bash
make setup
```  

Note that each plot is then saved as a PNG file in the `.fig` directory after being closed by the user. 

For a more detailed breakdown of the available commands to you, please see the [Available Makefile Commands](#available-makefile-commands) section.

## General Information

The primary source of exploration is a compilation of CMP energy usage data for the 7 meters associated with Austin Street Brewery Company. The are 5 columns and 500,279 rows. There is no missing data.

The data being analyzed has been extracted directly from the consumer-facing portal at *cmpco.com*. It has been structured and optimized using the Parquet format. This [README](data/cmp/curated/README.md) provides a brief rationale for our data storage decisions and describes some of the retrieval paradigms used with `pandas` and `pyarrow`. 

The location for this source's Parquet directory can be found [here](data/cmp/curated/meter-usage). To facilitate smooth development and execution as reproducibility, all commands are run out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** for this environment set to point directly to the `src` directory within this Conda environment. This allows a;ll scripts to easily import the `utils` module and its DataFrames and functions from any script within the `src` directory, including `meter_usage`.`

**Schema** 

  - `service_point_id` (**int**): A unique identifier for the point where the electrical service is provided, often tied to a specific location or customer.
  
  - `meter_id` (**str**): Identifier for the electrical meter installed at the service point. It records the amount of electricity consumed.
  
  - `interval_end_datetime` (**str**): Timestamp marking the end of the meter reading interval, typically indicating when the meter was read.
  
  - `meter_channel` (**int**): The channel number on the electrical meter. Meters with multiple channels can record different types of data.
  
  - `kwh` (**float**): Kilowatt-hours recorded by the meter during the interval, representing the unit of electricity consumed.

  - `account_number` (**int**): A unique identifier for the customer's account with CMP.

**Sample**

Using `.head()` on `meter_usage` yields the following results:

|    |   service_point_id | meter_id   | interval_end_datetime   |   meter_channel |   kwh |   account_number |
|---:|-------------------:|:-----------|:------------------------|----------------:|------:|-----------------:|
|  0 |         2300822246 | L108605388 | 10/1/2022 12:00:00 AM   |              10 | 0.594 |      30010320353 |
|  1 |         2300822246 | L108605388 | 10/1/2022 12:15:00 AM   |              10 | 0.101 |      30010320353 |
|  2 |         2300822246 | L108605388 | 10/1/2022 12:30:00 AM   |              10 | 0.104 |      30010320353 |
|  3 |         2300822246 | L108605388 | 10/1/2022 12:45:00 AM   |              10 | 0.106 |      30010320353 |
|  4 |         2300822246 | L108605388 | 10/1/2022 1:00:00 AM    |              10 | 0.099 |      30010320353 |