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
- [Available Makefile Commands](#available-makefile-commands)
- [Analysis](#analysis)
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

1. Operational cost mapping to understand which meters should power which equipment.
2. Benchmarking against industry standards to identify areas for improvement.
3. Analysis of total energy usage to compare the cost-efficiency of solar versus conventional suppliers.
4. Unbiased peak hour and supplier recommendations based on unsupervised machine learning models.


## Installation & Usage

This project utilizes [**Conda**](https://docs.conda.io/en/latest/) for managing dependencies and environments. Conda enables the creation of isolated environments that can house specific versions of packages, making it easier to manage complex projects. It's particularly well-suited for Python-based data science projects, ensuring that all dependencies are compatible and can be easily installed or removed.

### Setting Up the Environment

For the shortest path to getting up and running, you can simply run the command below, which will create the Conda environment and ensure the pathing is set up properly for subsequent commands and ad hoc querying.

```bash
make setup
```  

Note that each script containing a plot saves a PNG file in the `.fig` directory after being closed by the user. 

For a more detailed breakdown of the available commands to you, please see the [**Available Makefile Commands**](#available-makefile-commands) section.


## Available Makefile Commands

Each of these commands can be called from the CLI with the keyword construction `make {command}`.

- **`create-env`**: Creates a new Conda environment using the `environment.yml` file, installing all necessary Python packages and dependencies for the project.
- **`remove-env`**: Removes the Conda environment, deleting all Python packages and dependencies that were installed.
- **`run-all`**: A convenience command that sequentially executes `q1` through `q5` to perform all analyses in one command.
- **`setup`**: A composite command that creates the environment with `create-env` and then runs all analysis scripts with `run-all`.
- **`update-env`**: Updates the Conda environment according to the `environment.yml` file, ensuring all Python packages and dependencies are current.


## Analysis

## Acknowledgments

This project was shaped under the supervision of [**Professor Philip Bogden**](https://www.khoury.northeastern.edu/people/philip-bogden/) during our *Intro to Data Management* class at the **Roux Institute of Northeastern University**. 

We would like to express our gratitude to **Professor Bogden** for his consistent guidance and invaluable insights. Our thanks also extend to our TA, **Meghana Chillara**, for her patience, consistently timely input, and collaboration with each of us. Special thanks to both **Harsh Bhojwani** and **Anurag Daga** as well, for helping us keep our spirits high and reminding us to take a step back at the most opportunite times.