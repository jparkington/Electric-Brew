<!-- omit in toc -->
# Using SQL Instead of Python

Our project includes a database gereated with SQLite3, `electric_brew.db`, offering a structured and queryable format for our collected data. This README section highlights the key features of this database and guides you on how to interact with it effectively.

The **Entity-Relationship Diagram (ERD)** below visualizes the database schema, showcasing how different tables relate to each other. It provides an intuitive understanding of our database structure, making it easier for you to write queries and understand the underlying data relationships.

![ERD for Electric Brew Database](/fig/lineage/erd.drawio.svg)

## Why SQLite3?

- **Lightweight and Portable**: SQLite3 databases are self-contained and don't require a separate server, making them ideal for development, testing, and deployment in our project environment.
- **Ease of Use**: With its simple setup and familiar SQL syntax, SQLite3 allows our team to quickly query and manipulate data without the complexity of larger database systems.
- **Integration with Python**: SQLite3's seamless integration with Python makes it a natural choice for our data processing and analysis workflows, especially within our existing Python-based ecosystem.

## Accessing the Database

You can interact with the `electric_brew.db` using various tools and programming languages. Here's a quick Python example for connecting to and querying the database with `pandas`:

```python
import pandas  as pd
import sqlite3 as sq

# Connect to the SQLite3 database
# Note: You'll need to update your relative path to reflect where you run your script from
conn = sq.connect('data/sql/electric_brew.db')

# Using Pandas to execute an SQL query and directly read into a DataFrame
df_meter_usage = pd.read_sql_query("SELECT * FROM meter_usage", conn)

# Display the first few rows of the DataFrame
print(df_meter_usage.head())

# Close the connection
conn.close()
```

## Database Schema

The database is structured into several key tables, each serving a specific purpose in our data model. For a detailed data dictionary of all the fields available in these tables, please refer to the [DataFrame Dictionary](/src/README.md#dataframes) in our Utility READ ME.

**Key Tables**

Processed, but unaggregated source tables:

- `meter_usage`: Contains detailed records of electricity usage.

- `locations`: Stores location-based information for CMP accounts.

- `cmp_bills`: Aggregates billing information from various suppliers.

Tables modeled into a star schema:

- `dim_datetimes`: Breaks down timestamps into individual components.

- `dim_meters`: Aggregates meter-specific information.

- `dim_suppliers`: Contains information about energy suppliers.

- `fct_electric_brew`: Captures detailed records of electricity usage and associated costs.

**Usage Tips:**

- Use standard SQL queries to interact with the database.
- The ERD above is a great reference for understanding table relationships and designing complex queries.
- For bulk data operations, consider either using the existing DataFrames that match to these tables or loading our Parquet data directly for improved performance. You can learn more about other capabilities the project offers [here](../README.md).