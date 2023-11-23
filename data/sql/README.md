<!-- omit in toc -->
# Using SQL Instead of Python

Our project includes a database gereated with DuckDB, `electric_brew.db`, offering a structured and queryable format for our collected data. This README section highlights the key features of this database and guides you on how to interact with it effectively.

The **Entity-Relationship Diagram (ERD)** below visualizes the database schema, showcasing how different tables relate to each other. It provides an intuitive understanding of our database structure, making it easier for you to write queries and understand the underlying data relationships.

![ERD for Electric Brew Database](/fig/lineage/erd.drawio.svg)

## Why DuckDB?

- **Columnar Speed**: DuckDB provides excellent performance for OLAP queries, particularly beneficial for our Parquet-based data model.
- **Ease of Integration**: DuckDB's Python integration aligns well with our existing data processing and analysis workflows.
- **In-Process Management**: Unlike traditional databases that require separate servers, DuckDB runs in-process, simplifying deployment and usage within our project environment.

DuckDB comes pre-installed within the `electric-brew` Conda environment, ensuring immediate availability for project use without additional setup.

## Accessing the Database

You can interact with the `electric_brew.db` using various tools and programming languages. Here's a quick Python example for connecting to and querying the database with `pandas`:

```python
import duckdb

# Connect to the DuckDB database (or create it if it doesn't exist)
conn = duckdb.connect('data/sql/electric_brew.db')

# Directly run a SQL query and read the result into a DataFrame
df_meter_usage = conn.execute("SELECT * FROM meter_usage").fetchdf()

# Display the first few rows of the DataFrame
print(df_meter_usage.head())
```

## Database Views and Direct Parquet Querying

DuckDB allows for the creation of SQL views that directly query our Parquet files. This integration offers several significant benefits:

1. **Real-Time Data Reflection**: Views are dynamically updated as the underlying Parquet files change, ensuring up-to-date data access.
2. **Simplified Access**: These views provide an intuitive SQL interface to the complex structures of Parquet data.
3. **Enhanced Performance**: Leveraging DuckDB's columnar processing capabilities, direct Parquet querying results in faster data retrieval and analysis.

**Key Views**

Each DataFrame listed in the [#dataframes](#dataframes) section of our documentation is represented as a view within the database. These views serve as direct pointers to the Parquet files in their respective directories, allowing for efficient and up-to-date data querying.

## Running SQL on Pandas DataFrames

A unique feature of DuckDB is its ability to execute SQL queries directly on any existing Pandas DataFrames in the same process or script. This functionality allows for the integration of SQL-based data manipulation within Python scripts, enhancing flexibility and efficiency:

```python
import pandas as pd
import duckdb

# Example of a Pandas DataFrame
df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})

# Execute a SQL query directly on the DataFrame
result = duckdb.query("SELECT a, b FROM df WHERE a > 1").to_df()

print(result)
```

## Database Schema

The database is structured into several key tables, each serving a specific purpose in our data model. For a detailed data dictionary of all the fields available in these tables, please refer to the [DataFrame Dictionary](/src/README.md#dataframes) in our Utility README.

**Key Tables**

Processed, but unaggregated source tables:

- `meter_usage`: Contains detailed records of electricity usage.

- `locations`: Stores location-based information for CMP accounts.

- `cmp_bills`: Aggregates billing information from various non-solar suppliers, with CMP as the delivery partner.

- `ampion_bills`: Aggregates billing information and delivery volume from Ampion, Austin Street's solar provider.

Tables modeled into a star schema:

- `dim_datetimes`: Breaks down timestamps into individual components.

- `dim_meters`: Aggregates meter-specific information.

- `dim_bills`: Contains detailed information about billing intervals and their rates and charges.

- `fct_electric_brew`: Captures detailed records of electricity usage and associated costs.

**Usage Tips**

- Use standard SQL queries to interact with the database.
- The ERD above is a great reference for understanding table relationships and designing complex queries.
- For bulk data operations, consider either using the existing DataFrames that match to these tables or loading our Parquet data directly for improved performance. You can learn more about other capabilities the project offers [here](../README.md).