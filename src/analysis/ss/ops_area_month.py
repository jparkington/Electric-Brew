## OPERATIONAL COST MAPPING: 

## This script is used to analyze the total kWh usage by operational area (meter) over time.
## It uses the fct_electric_brew table, which contains the total kWh usage and total cost for each meter for each hour of each day.
## The dim_meters table contains the operational area for each meter, and the dim_datetimes table contains the year and month for each hour.
## The script uses a SQL query to join the three tables and group the data by operational area, year, and month.
## It then uses the pivot_table() method to pivot the DataFrame so that each operational area is a column and each row is a year-month combination.
## Finally, it plots the data using matplotlib to generate a line chart showing the peaks and valleys of kWh usage over time for each operational area.
## OUTPUT: Line chart showing the total kWh usage by operational area (meter) over time.


import sys
import os

# Adding the src directory to the Python path to ensure that modules can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))


from utils.dataframes import fct_electric_brew, dim_meters, dim_datetimes, dim_bills, meter_usage
from utils.runtime import connect_to_db, setup_plot_params
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from utils.runtime import find_project_root

setup_plot_params()

fct_electric_brew = connect_to_db()

# Directly run a SQL query and read the result into a DataFrame
df_meter_usage = fct_electric_brew.query("SELECT * FROM meter_usage").to_df()


# looking at all electrical meters, summed KwH usage
query3 = """
SELECT dm.meter_id, dd.year, dd.month, dm.operational_area,
       SUM(kwh) AS total_kWh_usage, SUM(total_cost) AS total_cost
FROM fct_electric_brew fct   
JOIN dim_meters dm ON fct.dim_meters_id = dm.id
JOIN dim_datetimes dd ON fct.dim_datetimes_id = dd.id
GROUP BY dm.meter_id, dd.year, dd.month, dm.operational_area,
ORDER BY dm.meter_id, dd.year, dd.month, dm.operational_area
"""

df_total_usage_by_ops_area = fct_electric_brew.execute(query3).fetch_df()

conn = duckdb.connect(database=':memory:')  # Connect to DuckDB
df = fct_electric_brew.execute(query3).fetch_df() 

# Preprocessing the data
df['year_month'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))

# Pivot the DataFrame
pivot_df = df.pivot_table(index='year_month', columns='operational_area', values='total_kWh_usage')

# Plotting
plt.figure(figsize=(12, 6))
for column in pivot_df.columns:
    plt.plot(pivot_df.index, pivot_df[column], label=column)

# Adding dashed lines at the start of each year
for year in range(df['year'].min(), df['year'].max() + 1):
    start_of_year = pd.to_datetime(str(year) + '-01-01')
    if start_of_year in pivot_df.index:
        plt.axvline(start_of_year, color='gray', linestyle='--', linewidth=1)

# Formatting x-axis labels
plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.xticks(rotation=45)

plt.title('Total kWh Usage by Operational Area (Meter) Over Time')
plt.xlabel('Year-Month')
plt.ylabel('Total kWh Usage')
plt.legend(title='Operational Area')
plt.tight_layout()
plt.show()


