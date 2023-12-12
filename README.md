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

In the Analysis section, we delve into the interconnected themes of operational efficiency, cost management, and sustainable energy use at Austin Street Brewery. Our approach reveals the complex interplay between energy consumption patterns, cost implications, and the potential of renewable energy sources. We uncover insights into how different operational areas contribute to overall energy usage, identifying specific periods and practices that drive up costs. 

This analysis is not just about pinpointing areas of high consumption but also understanding the underlying factors contributing to these patterns. We then explore the financial and environmental aspects of the brewery's energy mix, evaluating the current balance between conventional and solar energy and projecting future scenarios. The culmination of our analysis offers strategic recommendations, leveraging advanced modeling to propose optimizations in energy usage and supplier relationships. 

Throughout this section, our focus remains on providing a holistic view that blends operational practicality with a vision for sustainable growth, offering Austin Street Brewery a roadmap to not only reduce costs but also enhance their environmental footprint.


### Exploratory Data Analysis

```bash
make jp01
```
![01 - kWh vs. Total Cost](<./fig/analysis/jp/01 - kWh vs. Total Cost.png>)

Our analysis of the relationship between kilowatt-hours (kWh) and total cost is pivotal in strategizing for maximum energy efficiency at the lowest possible cost, a relationship that underpins each of our project's objectives. The data, encompassing over 335,000 entries, indicates considerable variability in both energy usage and cost, as evidenced by the average kWh usage of **0.845** with a standard deviation of **1.295**, and an average total cost of **$0.13** per reading with a standard deviation of **$0.23**. This variability is further underscored by the right-skewed distribution for both metrics, pointing to infrequent but significant spikes in energy usage and cost.

A key insight from this analysis is the strong positive correlation of **0.846** between kWh and total cost. This correlation, while indicative of a general trend where increased energy usage leads to higher costs, also reveals the intricacies of this relationship due to the static rates from CMP, suppliers, and Ampion. The pattern-driven, striated nature observed in the scatter plot underscores that the cost dynamics are not simply linear but are influenced by these structured rate tiers.

These insights suggest a huge opportunity for strategic operational adjustments, specifically targeting periods where high energy usage does not proportionally equate to high costs. Identifying these cost-efficient periods, possibly due to favorable rate structures or operational efficiencies, can lead to significant savings.

<br>

```bash
make jp02
```
![02 - Hourly Variation of kWh by Month](<./fig/analysis/jp/02 - Hourly Variation of kWh by Month.png>)

Following our examination of the kWh versus total cost relationship, the analysis of hourly kWh usage by month uncovers its stark seasonal variations, reflecting the brewery's heightened energy demands during the colder months, likely exacerbated by the need for heating during Maine's harsh winters. That necessity had become even more pronounced in the context of COVID-19, whcih we'll explore in later charts.

From the data, we observe that the energy usage peaks in the winter months, with January and December recording the highest average kWh usage at **0.94** and **0.96** respectively. This trend is indicative of increased heating requirements and possibly a higher operational pace during the holiday season. In contrast, the milder months of April and May show the lowest energy consumption, averaging at **0.75** and **0.73** kWh. The hourly breakdown within each month further reveals a consistent pattern of energy usage ramping up during the morning hours, peaking in the midday to early afternoon, and then gradually declining towards the night. This pattern aligns with typical operational hours and suggests that most energy-intensive activities occur during the day, tapering off as the brewery closes for the night.

Aligning energy-intensive processes with periods of lower kWh usage, especially during off-peak hours, could save on operational costs, assuming variables rates are available for Austin Street to opt into. Implementing energy storage solutions could also be beneficial, allowing the brewery to store energy during off-peak hours for use during peak demand times.

<br>

```bash
make jp03
```
![03 - Average Cost by Period Over Time](<./fig/analysis/jp/03 - Average Cost by Period Over Time.png>)

Building on our earlier analyses, the scatter plot depicting average cost by period over time at Austin Street Brewery underscores the nuances in their energy usage, particularly under the lens of peak and off-peak hours. A key observation from the data is the brewery's tendency towards higher energy usage during peak hours, as reflected in the average costs: **$0.14** for mid-peak, **$0.09** for off-peak, and **$0.13** for on-peak hours. This pattern is pivotal, as it suggests that a straightforward transition to a variable rate plan, without adjusting operational practices, might not be as beneficial as hoped.

In short, the concentration of data points during peak hours in the scatter plot indicates that the brewery's current energy-intensive activities align with generally more expensive energy periods. This presents an opportunity for strategic operational adjustments, such as rescheduling certain activities to off-peak hours, which could lead to more substantial cost savings. This approach, however, needs to be balanced with the practicalities of brewery operations, ensuring that any changes do not adversely affect productivity or product quality.

<br>

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

We would like to express our gratitude to **Professor Bogden** for his consistent guidance and invaluable insights. Our thanks also extend to our TA, **Meghana Chillara**, for her patience, consistently timely input, and collaboration with each of us. Special thanks to both **Harsh Bhojwani** and **Anurag Daga** as well, for helping us keep our spirits high and reminding us to take a step back at the most impactful times.