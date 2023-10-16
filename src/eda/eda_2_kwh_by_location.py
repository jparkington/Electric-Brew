from eda_features import meter_usage_engineered as mue
from utils import set_plot_params

import matplotlib.pyplot as plt

colors = set_plot_params()

# Define colors for each unique location
location_colors = {'Industrial Way': colors[0], 'Fox Street': colors[1]}

for location, color in location_colors.items():
    subset = mue[mue['location'] == location]
    plt.scatter(subset['interval_end_datetime'], 
                subset['kwh'], 
                color = color, 
                label = location, 
                alpha = 0.3, 
                s     = 10)

plt.title('Scatter Plot of kWh Usage Over Time Colored by Location')
plt.ylabel('kWh')
plt.xlabel('Datetime')
plt.legend(title = 'Location')

plt.tight_layout()
plt.savefig("fig/eda/kwh_by_location.png")
plt.show()
