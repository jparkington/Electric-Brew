from eda_features  import meter_usage_engineered as mue
from utils.runtime import setup_plot_params

import matplotlib.pyplot as plt
import numpy as np

colors = setup_plot_params()

# Map each meter_id to a unique color
unique_meters = mue['meter_id'].unique()
color_map = dict(zip(unique_meters, reversed(colors)))

# Scatter plot for each meter_id
for meter, color in color_map.items():
    subset = mue[mue['meter_id'] == meter]
    plt.scatter(subset['interval_end_datetime'], 
                subset['kwh'], 
                color = color, 
                label = meter, 
                alpha = 0.5, 
                s     = 10)

# Plot settings
plt.title('Scatter Plot of kWh Usage Over Time Colored by Meter ID')
plt.ylabel('kWh')
plt.xlabel('Datetime')
plt.legend(title = 'Meter ID')

plt.tight_layout()
plt.savefig("fig/eda/kwh_over_time_by_meter.png")
plt.show()
