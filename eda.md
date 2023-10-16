<!-- omit in toc -->
# Explortatory Data Analysis  

<!-- omit in toc -->
## Table of Contents

- [Introduction](#introduction)
- [Installation \& Usage](#installation--usage)
  - [Setting Up the Environment](#setting-up-the-environment)
- [General Information](#general-information)
- [Feature Engineering](#feature-engineering)
  - [Fields Added or Altered](#fields-added-or-altered)
  - [Distribution of kWh by Meter ID \& Year](#distribution-of-kwh-by-meter-id--year)
  - [Key Takeaways](#key-takeaways)
- [Mean and Max Usage Analysis](#mean-and-max-usage-analysis)
  - [Plot Aggregations](#plot-aggregations)
  - [Key Takeaways](#key-takeaways-1)
- [Energy Spikes](#energy-spikes)
  - [Key Takeaways](#key-takeaways-2)

## Introduction

The following is an exploratory data analysis of energy usage data for Austin Street Brewery Company. The raw data has been procured directly from Central Maine Power (CMP), which is Austin Street's principal energy supplier. 

## Installation & Usage

This project utilizes [**Conda**](https://docs.conda.io/en/latest/) for managing dependencies and environments. Conda enables the creation of isolated environments that can house specific versions of packages, making it easier to manage complex projects. It's particularly well-suited for Python-based data science projects, ensuring that all dependencies are compatible and can be easily installed or removed.

### Setting Up the Environment

For the shortest path to getting up and running, you can simply run the command below, which will create the Conda environment and run commands for each EDA script generated for this project:

```bash
make setup
```  

Charts can then be created and reviewed using one of the following commands:

```bash
make eda1
```

Note that each plot is then saved as a PNG file in the `.fig` directory after being closed by the user. 

## General Information

The primary source of exploration is a compilation of CMP energy usage data for the 7 meters associated with Austin Street Brewery Company. The are 5 columns and 500,279 rows. There is no missing data.

The data being analyzed has been extracted directly from the consumer-facing portal at *cmpco.com*. It has been structured and optimized using the Parquet format. This [README](data/cmp/curated/README.md) provides a brief rationale for our data storage decisions and describes some of the retrieval paradigms used with `pandas` and `pyarrow`. 

The location for this source's Parquet directory can be found [here](data/cmp/curated/meter-usage). To facilitate smooth development and execution as reproducibility, all commands are run out of the Conda environment created for the project, `electric-brew`. The **PYTHONPATH** for this environment set to point directly to the `src` directory within this Conda environment. This allows all scripts to easily import the `utils` module and its DataFrames and functions from any script within the `src` directory, including `meter_usage`.`

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

## Feature Engineering

This section discusses the reformatting of existing columns and the generation of new ones to enable more in-depth analysis. 

### Fields Added or Altered

  - `interval_date_time` (**datetime**): Converted to datetime datatype to facilitate time-based analysis. Enables granular, time-based comparisons and trend observations.
  
  - `year` (**int**): Year extracted from `interval_date_time` and added as a separate column. Useful for annual comparative analysis and identifying yearly trends.
  
  - `month` (**int**): Month extracted from `interval_date_time` and added as a separate column. Allows for monthly trend analysis and comparisons.
  
  - `month_name` (**str**): Full name of the month extracted for better readability in plots and reports.
  
  - `kwh_normalized` (**float**): Normalized energy usage, calculated as $\frac{{\text{kWh} - \mu}}{{\sigma}}$ where $\mu$ is the mean and $\sigma$ is the standard deviation, both computed grouped by `meter_id`. This normalization levels the playing field for analysis, allowing for a fair comparison across different meters and times.
  
  - `extreme_outlier` (**bool**): Boolean flag indicating if `kwh_normalized` is greater than 3 or less than -3. Serves as an immediate and accessible flag for unusual or extreme energy usage patterns.

### Distribution of kWh by Meter ID & Year

This section presents a series of boxplots, each representing the distribution of normalized kilowatt-hour (kWh) usage by meter ID for different years. The plot aims to provide insights into the variability, central tendency, and outliers in the energy consumption recorded by each meter ID annually.

![Distribution of kWh by Meter ID & Year](fig/eda/distribution_of_kwh.png)

### Key Takeaways

1. **Ubiquitous Energy Spikes Across Locations**: The energy consumption spikes are not isolated incidents but occur across all meters and both brewery locations. This uniformity suggests that if there are opportunities for using energy more efficiently, those strategies would likely be applicable company-wide, including potentially rescheduling equipment to operate during off-peak hours.

2. **Direct and Long-Term Cost Implications**: These spikes in energy usage are not merely statistical outliers; they have immediate and long-term financial ramifications. Not only do they increase the direct cost of energy, but they also risk pushing the brewery into a higher tariff bracket, which could inflate energy costs over a more extended period.

3. **Systemic Nature of Spikes**: The uniform distribution of spikes across meters and years implies that this is not a localized issue but a systemic one. A more in-depth analysis of how the energy grid and machinery interact could provide insights into better space and grid utilization. Adapting operations to these insights could offer a protective buffer against unexpected, costly spikes in energy demand.

## Mean and Max Usage Analysis

This section delves into the analysis of mean and max energy usage by individual meters, categorized by month and year. The analysis aims to identify patterns, spikes, and potential areas for operational optimization. 

### Plot Aggregations

- `max_usage` (**float**): Represents the highest energy consumption in kilowatt-hours (kWh) recorded by each meter in 15-minute intervals, grouped by month and year. This metric is crucial for understanding the extreme peaks in energy usage, which can have both immediate and long-term financial ramifications, such as pushing the brewery into a higher tariff bracket.

- `mean_usage` (**float**): Represents the average energy consumption in kilowatt-hours (kWh) for each meter, also grouped by month and year. Understanding the mean usage helps to gauge the typical energy needs and offers a baseline for potential energy-saving strategies.

- `max_mean_diff` (**float**): Calculates the percent difference between `max_usage` and `mean_usage`. A high percent difference suggests spikes in energy usage that are not merely statistical outliers but have systemic implications. This uniform distribution of spikes across meters and years implies the need for a more in-depth analysis to better utilize space and grid.  

![Max Usage Per 15-Minute Interval](fig/eda/max_usage.png)
![Mean Usage Per 15-Minute Interval](fig/eda/mean_usage.png)
![Percent Difference Between Mean & Max](fig/eda/max_mean_diff.png)

### Key Takeaways

1. **Operation-Induced Peaks**: The significant gap between `max_usage` and `mean_usage` suggests that the brewery experiences sporadic surges in energy demand. These could be attributed to high-energy processes like brewing cycles, sterilization, or refrigeration adjustments that are not continuous but occur at specific times. The `max_usage` captures these peaks, while the `mean_usage` offers a more 'typical' energy usage pattern. Understanding this disparity could be crucial for optimizing energy-intensive processes and, in turn, lowering overall operational costs.

2. **Strategic Mitigation**: The uniform nature of these spikes across all meters suggests that any mitigation strategies, such as energy-efficient initiatives or operational tweaks, could be applied company-wide.

3. **Operational Mapping**: The data calls for a deeper dive into the business operations linked to these usage spikes. Identifying the causes can lead to targeted strategies to mitigate their impact, thereby optimizing energy costs.

## Energy Spikes

Defining an energy spike as an interval where power usage exceeds $3 \sigma$ from the mean $\mu$ of `kwh` by `meter_id`, our dataset reveals **9,329** such instances out of its 500,000 recordings. While we will delve into the parameters and impacts in greater detail, this prevalence itself is noteworthy.

The subsequent visualizations aim to spotlight the frequency and temporal distribution of these energy spikes across individual meters.

![Count of Energy Spikes](fig/eda/count_of_spikes.png)
![Energy Spikes by Meter ID & Year](fig/eda/spikes_by_year.png)

### Key Takeaways

1. **Spike Frequency and Timing**: The frequency and timing of these spikes could reveal patterns tied to specific operational processes or schedules. Understanding when spikes are most likely to occur could inform better resource allocation or even preventative measures.

2. **Operational Constraints and Solutions**: The existence of multiple spikes indicates either operational inefficiency or a necessity dictated by the production cycle. Identifying the root cause could lead to tailored solutions, such as rescheduling high-energy processes to off-peak hours to capitalize on lower energy rates.

3. **Duration of Spikes**: Although not covered in this initial EDA, understanding the duration of these spikes could be critical. A prolonged spike might indicate a more systemic issue requiring a fundamental change, whereas short, frequent spikes could potentially be smoothed out via operational tweaks.

As this project advances, we will explore the duration and impact of these spikes in greater depth, aiming to offer actionable insights to mitigate their cost and operational implications.
