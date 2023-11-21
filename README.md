<!-- omit in toc -->
# Electric Brew
*NEEFC Energy Efficiency Project for Austin Street Brewery*

<!-- omit in toc -->
## Table of Contents

- [Project Team](#project-team)
- [Stakeholders](#stakeholders)
- [Story](#story)
- [Proposed Deliverables](#proposed-deliverables)
- [Data Sources](#data-sources)
  - [Operational Cost Mapping](#operational-cost-mapping)
  - [Industry Benchmarking](#industry-benchmarking)
  - [Peak Hour Optimization](#peak-hour-optimization)
  - [ROI Analysis](#roi-analysis)
  - [Additional Sources to Consider](#additional-sources-to-consider)
- [Acknowledgments](#acknowledgments)

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

In collaboration with Luke Truman from the New England Environmental Finance Center (NEEFC), whom has a proven track record in aiding craft beverage producers in enhancing energy efficiency and environmental sustainability, Austin Street Brewery aims to leverage data science to:
- Understand and optimize their current energy consumption patterns,
- Evaluate the financial and environmental gains of scaling up their reliance on community solar power,
- Benchmark their practices against industry standards, such as the Brewers Association Benchmarking Index, and
- Develop a solid analytics foundation that will support both current optimization and future growth.

Through this strategic partnership, Austin Street Brewery aspires not only to enhance operational efficiency but also to set new benchmarks in sustainable practices for the craft brewing industry.

## Proposed Deliverables
1. **Operational Cost Mapping**: A comprehensive analysis to map electricity consumption costs to specific operational areas. This will answer critical questions like which meters should be powering which machines to optimize costs.

2. **Industry Benchmarking**: An in-depth analysis comparing Austin Street Brewery's energy consumption and costs to industry benchmarks sourced from the Brewers Association Benchmarking Index. This will identify areas for improvement and potential cost savings.

3. **Peak Hour Optimization**: Data-driven recommendations focusing on energy consumption during peak hours. The aim is to suggest strategies for reducing energy costs and consumption during these high-usage periods.

4. **Stretch Goal - ROI Analysis**: A profitability analysis exploring various thresholds of upfront energy investment. This will provide insights into the long-term financial and environmental benefits of different energy optimization strategies.

## Data Sources

### Operational Cost Mapping
- **Electricity Usage Data**: Detailed consumption data from 7 separate meters within Austin Street Brewery's two locations, stored in .csv format. Each meter corresponds to different operational facets of the brewery.
  
- **Operational Process Data**: Information on the specific machines or processes powered by each of the 7 meters. This could be obtained through internal documentation or asset management software like [IBM Maximo](https://www.ibm.com/products/maximo).

### Industry Benchmarking
- **Utility Bills**: Invoices from electric utility companies like [Central Maine Power (CMP)](https://www.cmpco.com/) and [Unitil](https://unitil.com/).
  
- **Sustainability Benchmarking Data**: Metrics and key performance indicators sourced from the [Brewers Association Benchmarking Survey](https://www.brewersassociation.org/).

### Peak Hour Optimization
- **Granular Energy Data**: Data from [CMP's smart meter system](https://www.cmpco.com/).
  
- **Time-of-Use Rates**: Information on variable electricity rates can be obtained from utility companies or through Maine's [Public Utilities Commission](https://www.maine.gov/mpuc/).

### ROI Analysis
- **Solar Energy Data**: Statistics on the brewery's 15% energy sourcing from community solar power.
  
- **Investment Cost Data**: Data on the costs associated with various energy optimization strategies could be sourced from academic research or governmental reports focused on renewable energy. For Maine-specific data, we'll start by looking at publications from the [U.S. Energy Information Administration](https://www.eia.gov/) or [Database of State Incentives for Renewables & Efficiency (DSIRE)](https://www.dsireusa.org/).

### Additional Sources to Consider
- **Weather Data**: Historical weather data can be sourced from platforms like [NOAA's Climate Data Records](https://www.ncei.noaa.gov/products/climate-data-records).
  
- **Production Volume Data**: Information on the brewery's output can be sourced from the [Brewers Association Benchmarking Survey](https://www.brewersassociation.org/), which includes production volume metrics.

- **Reporting Requirements Data**: Information that aligns with [City of Portland's](https://www.portlandmaine.gov/) reporting requirements could be accessed through municipal databases or direct correspondence with city officials.

- **Brewery-Specific Case Studies on Energy Efficiency**: Reviews of key case studies from other breweries that have effectively reduced energy consumption and carbon footprint. Examples of such breweries and potential sources for these case studies include:
  - [New Belgium Brewing Company's](https://www.newbelgium.com/company/mission/climate/) sustainability reports, which detail their water and energy-saving initiatives.
  - [Sierra Nevada Brewing Co.'s](https://sierranevada.com/sustainability/) annual sustainability report that includes data on renewable energy usage.
  - [BrewDog's](https://www.brewdog.com/uk/tomorrow) "Make Earth Great Again" report, outlining their carbon-negative status and sustainability initiatives.

## Acknowledgments

This project was shaped under the supervision of [**Professor Philip Bogden**](https://www.khoury.northeastern.edu/people/philip-bogden/) during our *Intro to Data Management* class at the **Roux Institute of Northeastern University**. 

We would like to express our gratitude to **Professor Bogden** for his consistent guidance and invaluable insights. Our thanks also extend to our TA, **Meghana Chillar**a, for her patience, consistently timely input, and collaboration with each of us, and to Harsh Bhojwani and Anurag Daga for helping us keep our spirits high and helping us step back at the most opportunite times.