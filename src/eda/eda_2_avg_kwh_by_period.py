from eda_features import meter_usage_engineered as mue
from utils import set_plot_params

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

colors = set_plot_params()

# Calculate total kWh usage for each period
period_kwh = mue.groupby('period')['kwh'].sum()

# Define the number of hours in each period
hours_in_period = {'Off-peak: 12AM to 7AM'             : 7,
                   'Mid-peak: 7AM to 5PM, 9PM to 11PM' : 13,  
                   'On-peak: 5PM to 9PM'               : 4}

# Calculate the average hourly usage for each period
average_hourly_usage = period_kwh / pd.Series(hours_in_period)

# Define the order in which the periods should appear
period_order = ['Off-peak: 12AM to 7AM', 'Mid-peak: 7AM to 5PM, 9PM to 11PM', 'On-peak: 5PM to 9PM']

# Create the plot
sns.barplot(data    = pd.DataFrame({'period'      : average_hourly_usage.index,
                                    'average_kwh' : average_hourly_usage.values}),
            x       = 'period',
            y       = 'average_kwh',
            hue     = 'period',
            order   = period_order)

plt.title('Average kWh Usage per Hour by Period')
plt.ylabel('Average kWh per Hour')
plt.xlabel(None)
plt.tight_layout()
plt.savefig("fig/eda/avg_kwh_by_period.png")
plt.show()
