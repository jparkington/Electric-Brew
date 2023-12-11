<!-- omit in toc -->
# Electric Brew
*NEEFC Energy Efficiency Project for Austin Street Brewery*

<!-- omit in toc -->
## Table of Contents
- [Quick Links](#quick-links)
- [Project Team](#project-team)
- [Stakeholders](#stakeholders)
- [Story](#story)
- [Installation \& Usage](#installation--usage)
  - [Setting Up the Environment](#setting-up-the-environment)
  - [All Makefile Commands](#all-makefile-commands)
- [Analysis](#analysis)
  - [Exploratory Data Analysis](#exploratory-data-analysis)
  - [Operational Cost Mapping](#operational-cost-mapping)
  - [Energy Usage Analysis](#energy-usage-analysis)
  - [Peak Hour \& Supplier Modeling](#peak-hour--supplier-modeling)
- [Acknowledgments](#acknowledgments)

## Quick Links

Welcome to the Electric Brew Project! Below you'll find quick links to key documents and resources that provide in-depth information about various aspects of our data pipeline and infrastructure:

1. **[Data Dictionary](/docs/data_dictionary.md)**: Delve into our detailed data dictionary, which offers a complete overview of all the tables, fields, and their data types within the project.

2. **[Data Lineage and Parquet Tips](/data/README.md)**: Understand the lineage of our data and get practical tips for working with Parquet files, ensuring effective data management and analysis.

3. **[ERD and DuckDB Usage](/data/sql/README.md)**: Access our Entity-Relationship Diagram (ERD) for a visual representation of our data model, along with guidelines for leveraging DuckDB for your data queries and storage needs.

4. **[Data Pipeline Functions](/src/README.md)**: Explore the comprehensive list of functions utilized in our data pipeline, detailing their purposes and usage.
  

## Project Team
- **Sean Sullivan** ([@seanmainer](https://github.com/seanmainer))
- **Joseph Nelson Farrell** ([@nfarrell011](https://github.com/nfarrell011))
- **James Parkington** ([@jparkington](https://github.com/jparkington))


## Stakeholders
- [**Luke Truman**](https://neefc.org/our-team/): New England Environmental Finance Center (NEEFC), Sustainability Coordinator
- [**Will Fisher**](https://www.austinstreetbrewery.com/about): Austin Street Brewery, Co-founder & CEO


## Story
Austin Street Brewery, a thriving craft beer brewery situated in Maine, is at an exciting stage of its growth trajectory. With expansion comes opportunities for innovation, and the brewery's energy infrastructure is a prime example. Currently, energy consumption is measured through seven different electricity meters, each tied to various operational facets of the brewery, from brewing and packaging to maintaining a welcoming atmosphere in the tasting room.

As a forward-thinking establishment, Austin Street Brewery has already made strides towards sustainability by sourcing 15% of their energy from solar power and are keen to understand how they might increase this proportion effectively. With multiple supplier contracts and the complexity that comes with sustainable energy sources, there exists a unique opportunity to streamline their energy management practices.

In collaboration with Luke Truman from the New England Environmental Finance Center (NEEFC), whom has a proven track record in aiding craft beverage producers in enhancing energy efficiency and environmental sustainability, Austin Street Brewery aims to leverage data science for the following objectives:

1. [**Operational cost mapping**](#exploratory-data-analysis) to understand which meters should power which equipment.
2. Benchmarking against industry standards to identify areas for improvement.
3. Analysis of total [**energy usage**](#usage-analysis) to compare the cost-efficiency of solar versus conventional suppliers.
4. Unbiased [**peak hour and supplier**](#peak-hour--supplier-modeling) recommendations based on unsupervised machine learning models.


## Installation & Usage

This project utilizes [**Conda**](https://docs.conda.io/en/latest/) for managing dependencies and environments. Conda enables the creation of isolated environments that can house specific versions of packages, making it easier to manage complex projects. It's particularly well-suited for Python-based data science projects, ensuring that all dependencies are compatible and can be easily installed or removed.

### Setting Up the Environment

For the shortest path to getting up and running, you can simply run the command below, which will create the Conda environment and ensure the pathing is set up properly for subsequent commands and ad hoc querying.

```bash
make setup
```  

Note that each script containing a plot saves a PNG file in the `.fig` directory after being closed by the user.

The entire ETL process is ready for you upon cloning the repo. However, you can reproduce the scraping, curating, and modeling steps with the command below.

```bash
make etl
```

### All Makefile Commands

Each of these commands can be called from the CLI with the keyword construction `make {command}`.

**Environment Management with Conda**

- **`create-env`**: Creates a new Conda environment using the `environment.yml` file.
- **`remove-env`**: Removes the Conda environment, deleting all installed packages and dependencies.
- **`set-pythonpath`**: Sets the PYTHONPATH environment variable for the current Conda environment.
- **`setup`**: Composite command that creates the environment and sets PYTHONPATH.
- **`update-env`**: Updates the Conda environment as per the `environment.yml` file.

**ETL Pipeline**

- **`etl`**: Initiates the ETL pipeline, preparing the data for analytics.

**Initial Exploratory Data Analysis (EDA)**

- **`eda1`**: Executes EDA scripts for kWh distribution, usage patterns, and energy spikes.
- **`eda2`**: Runs EDA scripts for visualizing kWh usage by period, time, and location.

**Operational Cost Mapping**

- **`ss01`**: Generates a line chart of total kWh usage by operational area over time.
- **`ss02`**: Creates heatmaps for kWh usage by hour and month for each operational area.
- **`ss-all`**: Executes all scripts in 'Operational Cost Mapping' sequentially.

**Energy Usage Analysis**

- **`nf01`**: Visualizes total energy usage at Austin Street Brewery over the course of the dataset.
- **`nf02`**: Visualizes energy usage by different time intervals.
- **`nf03`**: Produces a breakdown of energy usage by generation type (solar vs. conventional).
- **`nf04`**: Produces a summary visual of cost per kWh across generation types.
- **`nf05`**: Generates projections for solar power costs for Austin Street Brewery.
- **`nf-all`**: Executes all scripts in 'Energy Usage Analysis' sequentially.

**Peak Hour & Supplier Modeling**

- **`jp01`**: Visualizes the relationship between kWh and Total Cost.
- **`jp02`**: Visualizes hourly variation of kWh usage by month.
- **`jp03`**: Visualizes average cost by period over time.
- **`jp04`**: Applies anomaly detection using Isolation Forest.
- **`jp05`**: Visualizes heatmap of high correlations among numeric columns.
- **`jp06`**: Applies feature selection using LASSO.
- **`jp07`**: Performs K-Means clustering and PCA visualization.
- **`jp08`**: Fits a Linear Regression model and visualizes results.
- **`jp09`**: Fits a Random Forest model and visualizes predictions.
- **`jp10`**: Compares cross-validation R² scores across models.
- **`jp11`**: Performs SLSQP optimization and visualizes results.
- **`jp12`**: Visualizes percentage changes in categorical values after optimization.
- **`jp-all`**: Executes all scripts in 'Peak Hour & Supplier Modeling' sequentially.

**All Analysis**
- **`analysis`**: Executes each of the analysis sections in order of the project's objectives.


## Analysis

### Exploratory Data Analysis

```bash
make jp01
```
![01 - kWh vs. Total Cost](<./fig/analysis/jp/01 - kWh vs. Total Cost.png>)

```bash
make jp02
```
![02 - Hourly Variation of kWh by Month](<./fig/analysis/jp/02 - Hourly Variation of kWh by Month.png>)

```bash
make jp03
```
![03 - Average Cost by Period Over Time](<./fig/analysis/jp/03 - Average Cost by Period Over Time.png>)


### Operational Cost Mapping

```bash
make ss01
```
![Total kWh Usage by Operational Area Over Time](<./fig/analysis/ss/Total kWh Usage by Operational Area Over Time.png>)

```bash
make ss02
```
![Avg. kWh Usage by Hour for Boiler Pump-Patio-Forklift](<./fig/analysis/ss/Avg. kWh Usage by Hour for Boiler Pump-Patio-Forklift.png>)
![Avg. kWh Usage by Hour for Brewpump](<./fig/analysis/ss/Avg. kWh Usage by Hour for Brewpump.png>)
![Avg. kWh Usage by Hour for Front of House](<./fig/analysis/ss/Avg. kWh Usage by Hour for Front of House.png>)
![Avg. kWh Usage by Hour for Industrial-1](<./fig/analysis/ss/Avg. kWh Usage by Hour for Industrial-1.png>)
![Avg. kWh Usage by Hour for Industrial-2](<./fig/analysis/ss/Avg. kWh Usage by Hour for Industrial-2.png>)
![Avg. kWh Usage by Hour for Industrial-3](<./fig/analysis/ss/Avg. kWh Usage by Hour for Industrial-3.png>)
![Avg. kWh Usage by Hour for Package-Hot-Chill](<./fig/analysis/ss/Avg. kWh Usage by Hour for Package-Hot-Chill.png>)


### Energy Usage Analysis

```bash
make nf01
```
![Total Energy Usage](/fig/analysis/nf/aggregated_usage_fig.png)

```bash
make nf02
```
![Usage by Time Interval](/fig/analysis/nf/usage_by_time_interval.png)


```bash
make nf03
```
![Energy Usage by Energy Generation Type](/fig/analysis/nf/supplier_fig.png)


```bash
make nf04
```
![Energy Costs](/fig/analysis/nf/aggregated_costs_fig.png)


```bash
make nf05
```
![Solar Projections](/fig/analysis/nf/projections_fig.png)


### Peak Hour & Supplier Modeling

```bash
make jp04
```
![04 - Applying Anomaly Detection with Total Cost](<./fig/analysis/jp/04 - Applying Anomaly Detection with Total Cost.png>)

```bash
make jp05
```
![05 - Multicollinear Facts with High Correlations](<./fig/analysis/jp/05 - Multicollinear Facts with High Correlations.png>)

```bash
make jp06
```
![06 - Feature Selection for Determining Total Cost](<./fig/analysis/jp/06 - Feature Selection for Determining Total Cost.png>)

```bash
make jp07
```
![07 - KMeans Clusters in Reduced Dimensional Space](<./fig/analysis/jp/07 - KMeans Clusters in Reduced Dimensional Space.png>)

```bash
make jp08
```
![08 - Linear Regression Predictions vs Actual Values](<./fig/analysis/jp/08 - Linear Regression Predictions vs Actual Values.png>)

```bash
make jp09
```
![09 - Random Forest Predictions vs Actual Values](<./fig/analysis/jp/09 - Random Forest Predictions vs Actual Values.png>)

```bash
make jp10
```
![10 - Cross-Validation R2 Scores Comparison](<./fig/analysis/jp/10 - Cross-Validation R2 Scores Comparison.png>)

```bash
make jp11
```
![11 - Distribution of Predicted Costs in Optimized Feature Sets](<./fig/analysis/jp/11 - Distribution of Predicted Costs in Optimized Feature Sets.png>)

```bash
make jp12
```
![12 - Percent Change in Categorical Features After Optimization](<./fig/analysis/jp/12 - Percent Change in Categorical Features After Optimization.png>)


## Acknowledgments

This project was shaped under the supervision of [**Professor Philip Bogden**](https://www.khoury.northeastern.edu/people/philip-bogden/) during our *Intro to Data Management* class at the **Roux Institute of Northeastern University**. 

We would like to express our gratitude to **Professor Bogden** for his consistent guidance and invaluable insights. Our thanks also extend to our TA, **Meghana Chillara**, for her patience, consistently timely input, and collaboration with each of us. Special thanks to both **Harsh Bhojwani** and **Anurag Daga** as well, for helping us keep our spirits high and reminding us to take a step back at the most opportunite times.