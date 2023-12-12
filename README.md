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
  - [Major Takeaways](#major-takeaways)
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

A standout observation in this chart is the significant power usage in areas associated with heating and cooling. The 'Boiler Pump/Patio/Forklift' area at Fox St. (represented in blue) shows marked spikes in power consumption during winter months. These spikes correlate with periods of COVID-19 when outdoor seating was prevalent, necessitating the use of patio heaters. Despite a reduction in outdoor seating, the energy usage remains high, likely driven by the boiler's continued operation for brewing processes.

Similarly, the 'Front of House' area (shown in green) exhibits increased energy usage during peak summer months, an interesting inverse to the Patio line. This trend likely reflects the heightened demand for air conditioning during this period. 

And finally, in the 'Industrial-3' line (highlighted in brown), there is a significant uptick in kWh usage starting in the summer of 2022. Upon consultation with stakeholders, we learned that this increase was due to the installation of a large walk-in cooler at the Industrial Way location.

These insights point to potential areas for energy efficiency improvements and cost savings. For instance, exploring more energy-efficient heating solutions for the patio area and optimizing the usage of cooling systems in the Front of House could yield some benefits, since they seem to be the major drivers of energy consumption at these two locations.

<br>

```bash
make ss02
```
<table>
  <tr>
    <td>
      <img src="./fig/analysis/ss/Avg. kWh Usage by Hour for Boiler Pump-Patio-Forklift.png" alt="Avg. kWh Usage by Hour for Boiler Pump-Patio-Forklift" style="width:100%">
    </td>
    <td>
      <img src="./fig/analysis/ss/Avg. kWh Usage by Hour for Package-Hot-Chill.png" alt="Avg. kWh Usage by Hour for Package-Hot-Chill" style="width:100%">
    </td>
  </tr>
  <tr>
    <td>
      <img src="./fig/analysis/ss/Avg. kWh Usage by Hour for Front of House.png" alt="Avg. kWh Usage by Hour for Front of House" style="width:100%">
    </td>
    <td>
      <img src="./fig/analysis/ss/Avg. kWh Usage by Hour for Industrial-3.png" alt="Avg. kWh Usage by Hour for Industrial-3" style="width:100%">
    </td>
  </tr>
</table>

Building upon our previous analysis, which highlighted significant power usage in areas associated with heating and cooling, the newly generated heatmaps not only further substantiate some of those initial observations, but they also showcase how inter-related operational patterns can be for different areas of the business.

1. **Boiler Pump/Patio/Forklift**: Consistent with our previous findings, the heatmap for this area shows pronounced energy usage in the winter months, especially during evenings. This aligns with the operational hours of patio heaters used during the COVID-19 period, when outdoor seating was essential.

2. **Front of House**: Also echoing our earlier observation, this area's energy consumption peaks in the summer, highlighting the significant use of air conditioning. The focused usage during these warmer months, particularly in business hours, emphasizes the need for cooling systems to maintain a comfortable environment, and presents an opportunity for exploring more energy-efficient cooling solutions.

3. **Package/Hot/Chill**: This area demonstrates a relatively stable energy usage throughout the year with a notable decrease in late summer, assumedly due to the installation of a walk-in cooler at Industrial Street no longer necessitating the same degree of chilling for this Fox Street meter.

4. **Industrial-3**: A marked increase in energy usage is observed during the summer months. This surge corresponds with our previous finding of the new walk-in cooler installation at the Industrial Way location, underscoring the impact of such capital improvements on the brewery's energy needs.

<br>

### Energy Usage Analysis

```bash
make nf01
```
![Total Energy Usage](/fig/analysis/nf/aggregated_usage_fig.png)

The data on total energy usage shows a gradual increase in kilowatt-hours over the years, with the lowest monthly usage recorded in October 2020 at **6,966** kWh and the peak in August 2023 at **15,244** kWh. This trend suggests a growing operational scale that mimics the brewery's expansion into more space over that period.

A key observation is the notable increase in average energy usage following the introduction of solar power in September 2022. Before the Ampion partnership, the average monthly energy usage was around **11,024** kWh. Post the solar power transition, this average rose to approximately **12,721** kWh. This increase can be attributed to several factors, including expanded operations, more energy-intensive processes, and some seasonal variation. 

<br>

```bash
make nf02
```
![Energy Usage by Energy Generation Type](/fig/analysis/nf/supplier_fig.png)

In looking more closely at the energy supply by generation type, the data distinctly shows that during summer months, the brewery derives a substantial portion of its energy from solar power, with peaks in solar energy usage notably in September and July, at **87.4%** and **70.5%** respectively. This trend is a testament to the brewery's successful implementation of solar power through its partnership with Ampion initiated in September 2022.

However, during winter months, the reliance on solar energy diminishes significantly, with the lowest solar energy usage recorded in December at merely **8.5%**. This seasonal variance is characteristic of Maine's climate, where solar energy generation is less efficient during shorter, cloudier winter days. The stark contrast in energy sourcing between summer and winter months reflects the natural limitations of solar power in the northeastern United States but also highlights an opportunity for Austin Street to optimize its energy management.

Given the higher solar energy production in summer, if the brewery were ability to bank energy credits, perhaps by enhancing solar energy storage capabilities, that could potentially yield better performance even in less ideal conditions.

<br>

```bash
make nf03
```
![Energy Costs Summary](/fig/analysis/nf/aggregated_costs_fig.png)

While embracing solar energy is a significant step towards sustainability, it currently comes at a higher monetary cost. The total energy costs over time show considerable variability, with a mean monthly cost of about **$1,175**. Breaking down the costs by generation type, solar energy consistently incurs higher expenses (mean: **$1,362**) compared to conventional sources (mean: **$923**). This consistent trend, despite the variability in costs, might be influenced by the efficiency and technology of solar power, particularly in Maine's seasonal climate.

The cost per kWh analysis sheds more light on this. Solar energy, across the analyzed period, has a higher per kWh cost compared to conventional sources. It's essential to consider the broader context of solar power's higher initial costs, including the assumed tax credits and long-term savings expected as part of its implementation. These factors, beyond the immediate financial comparison, may influence the overall viability of solar energy for the brewery. However, without in-depth knowledge of these incentives and the long-term economic impact, it's challenging to conclusively assess solar power's financial feasibility for Austin Street Brewery.

<br>

```bash
make nf04
```
![Solar Projections](/fig/analysis/nf/projections_fig.png)

In this analysis, we explore a hypothetical scenario where all of Austin Street's power is sourced from solar energy. The actual vs. projected energy costs reveal a clear trend that follows from the previous charts—switching entirely to solar power would have led to higher expenses for the brewery. Over the analyzed period, the average actual cost per month stands at about **$2,285**, whereas the projected costs under a full solar regime average around **$2,870**.

Interestingly, the cost per kWh for solar energy averages at **$0.234**, which, while relatively stable (std. dev.: **$0.027**), contributes to higher projected costs. This reaffirms that, under current conditions and pricing models, solar energy is more expensive per unit than the brewery's existing energy mix.

The percentage differences between actual and projected costs provide a nuanced perspective. In certain months like December 2022 and January 2023, the cost difference is strikingly high, reaching up to **74.58%** and **43.63%**, underscoring the substantial financial impact that a full shift to solar energy could have during months with higher energy demands.

In summary, while the transition to solar energy aligns with sustainability goals, it currently presents a significant cost increase for Austin Street Brewery. This analysis highlights the importance of a balanced approach, where solar energy complements rather than completely replaces conventional energy sources. It also emphasizes the need for further exploration into cost-effective solar solutions, potential subsidies, and incentives that could make a full transition more financially viable in the future.

<br>

### Peak Hour & Supplier Modeling

```bash
make jp04
```
![04 - Applying Anomaly Detection with Total Cost](<./fig/analysis/jp/04 - Applying Anomaly Detection with Total Cost.png>)

<br>

```bash
make jp05
```
![05 - Multicollinear Facts with High Correlations](<./fig/analysis/jp/05 - Multicollinear Facts with High Correlations.png>)

<br>

```bash
make jp06
```
![06 - Feature Selection for Determining Total Cost](<./fig/analysis/jp/06 - Feature Selection for Determining Total Cost.png>)

<br>

```bash
make jp07
```
![07 - KMeans Clusters in Reduced Dimensional Space](<./fig/analysis/jp/07 - KMeans Clusters in Reduced Dimensional Space.png>)

<br>

```bash
make jp08
```
![08 - Linear Regression Predictions vs Actual Values](<./fig/analysis/jp/08 - Linear Regression Predictions vs Actual Values.png>)

<br>

```bash
make jp09
```
![09 - Random Forest Predictions vs Actual Values](<./fig/analysis/jp/09 - Random Forest Predictions vs Actual Values.png>)

<br>

```bash
make jp10
```
![10 - Cross-Validation R2 Scores Comparison](<./fig/analysis/jp/10 - Cross-Validation R2 Scores Comparison.png>)

<br>

```bash
make jp11
```
![11 - Distribution of Predicted Costs in Optimized Feature Sets](<./fig/analysis/jp/11 - Distribution of Predicted Costs in Optimized Feature Sets.png>)

<br>

```bash
make jp12
```
![12 - Percent Change in Categorical Features After Optimization](<./fig/analysis/jp/12 - Percent Change in Categorical Features After Optimization.png>)

<br>

### Major Takeaways

1. **Solar is more expensive**, but that said, you can bank credits in the summer and use them in the winter.
   
2. **Heating and cooling dominate** the cost profile for the company, and should be a focus for cost optimization.
  
3. Because energy usage is greatest during peak hours, a **shift to a variable rate** (Mid/On/Off Peak) would be **more costly**.

4. **The structure of the data** lends itself well to prediction and an opportunity to revisit as the business changes.

## Acknowledgments

This project was shaped under the supervision of [**Professor Philip Bogden**](https://www.khoury.northeastern.edu/people/philip-bogden/) during our *Intro to Data Management* class at the **Roux Institute of Northeastern University**. 

We would like to express our gratitude to **Professor Bogden** for his consistent guidance and invaluable insights. Our thanks also extend to our TA, **Meghana Chillara**, for her patience, consistently timely input, and collaboration with each of us. Special thanks to both **Harsh Bhojwani** and **Anurag Daga** as well, for helping us keep our spirits high and reminding us to take a step back at the most impactful times.