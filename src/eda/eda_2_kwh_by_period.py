from eda_features import meter_usage_engineered as mue
from utils import set_plot_params

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

colors = set_plot_params()

# Group by period and sum kWh
period_kwh = mue.groupby('period')['kwh'].sum().reset_index()

# Define the order in which the periods should appear
period_order = ['Off-peak: 12AM to 7AM', 'Mid-peak: 7AM to 5PM, 9PM to 11PM', 'On-peak: 5PM to 9PM']

sns.barplot(data  = period_kwh, 
            x     = 'period', 
            y     = 'kwh',
            hue   = 'period', 
            order = period_order)

plt.title('Total kWh Usage by Period')
plt.ylabel('kWh')
plt.tight_layout()
plt.savefig("fig/eda/kwh_by_period.png")
plt.show()
