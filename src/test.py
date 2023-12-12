import pandas as pd
from utils.runtime import connect_to_db
import matplotlib.pyplot as plt


# Define the SQL query
query = """
    SELECT date,
           kwh
    FROM fct_electric_brew fe
    LEFT JOIN dim_datetimes dd ON fe.dim_datetimes_id = dd.id
"""

# Connect to the database
electric_brew = connect_to_db()

# Execute the query and save to a DataFrame
df = electric_brew.query(query).to_df()

# Process the DataFrame
df['month'] = df['date'].dt.to_period('M')
df = df.groupby('month')['kwh'].sum().reset_index()
df['month_name'] = df['month'].dt.strftime('%B')

# Print the DataFrame structure
print("DataFrame Head:\n", df.head())
print("DataFrame Tail:\n", df.tail())

# Print summary statistics for kWh
print("\nSummary Statistics for kWh:\n", df['kwh'].describe())

# Find the months with the highest and lowest energy usage
max_usage_month = df.loc[df['kwh'].idxmax()]
min_usage_month = df.loc[df['kwh'].idxmin()]
print("\nMonth with Maximum Energy Usage:\n", max_usage_month)
print("\nMonth with Minimum Energy Usage:\n", min_usage_month)

# Analyze energy usage before and after the introduction of the solar power supply (October 2022)
solar_start_date = pd.to_datetime('2022-10').to_period('M')
pre_solar_df = df[df['month'] <= solar_start_date]
post_solar_df = df[df['month'] > solar_start_date]
print("\nAverage Energy Usage Before Solar Power Supply Start:\n", pre_solar_df['kwh'].mean())
print("\nAverage Energy Usage After Solar Power Supply Start:\n", post_solar_df['kwh'].mean())
