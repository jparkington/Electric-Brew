## OPERATIONAL COST MAPPING: 
## Purpose: Generate a heatmap for each operational area showing kWh usage by hour and month.
## OUTPUT: This script will generate 7 plots, one for each operational area, showing a heatmap which displays kWh usage by hour and month.


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import calendar

import sys
import os

# Adding the src directory to the Python path to ensure that modules can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.dataframes import fct_electric_brew, dim_meters, dim_datetimes, dim_bills, meter_usage
from utils.runtime import connect_to_db, setup_plot_params

# set plot params to align with all electric_brew plots
setup_plot_params()

fct_electric_brew = connect_to_db()

# Selecting the hour and month, averaging usage to account for multiple years of data, 
# will be executed for each operational_area to generate a heatmap showing kWh usage by hour and month for each operational area
query_template = """
SELECT dd.hour, dd.month, dm.operational_area,
       AVG(kwh) AS avg_kWh_usage
FROM fct_electric_brew fct   
JOIN dim_meters dm ON fct.dim_meters_id = dm.id
JOIN dim_datetimes dd ON fct.dim_datetimes_id = dd.id
WHERE dm.operational_area = '{operational_area}'
GROUP BY dd.hour, dd.month, dm.operational_area
ORDER BY dd.hour, dd.month, dm.operational_area
"""

# Get the list of unique operational areas
unique_areas = pd.read_sql("SELECT DISTINCT operational_area FROM dim_meters", fct_electric_brew)

# Loop through each operational area and generate a heatmap
for area in unique_areas['operational_area']:
    # Format the SQL query for the current operational area
    area_query = query_template.format(operational_area=area)
    
    # Fetch the DataFrame for the current operational area
    df_area = fct_electric_brew.execute(area_query).fetch_df()

    # Pivot the DataFrame for average kWh usage
    heatmap_data = df_area.pivot_table(index='hour', columns='month', values='avg_kWh_usage', aggfunc='mean')

    # Create the heatmap
    plt.figure(figsize=(12, 6))
    p = sns.heatmap(heatmap_data, cmap='cividis', cbar_kws={'label': 'Avg. kWh Used'})

    # Set the month names as labels
    month_names = [calendar.month_name[i] for i in heatmap_data.columns]
    p.set_xticklabels(month_names, rotation=45)

    # Set the title and labels
    p.set_title(f'Avg. kWh Usage by Hour for {area}')
    p.set_xlabel('Month')
    p.set_ylabel('Hour of Day')

    plt.tight_layout(pad=2.0)
    plt.show()