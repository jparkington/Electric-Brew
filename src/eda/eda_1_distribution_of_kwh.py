from eda_1_feature_engineering import meter_usage_engineered as mue
from utils import set_plot_params

import matplotlib.pyplot as plt
import numpy             as np
import seaborn           as sns  

colors = set_plot_params() # Enables dark mode, larger canvas sizes, and sizing considerations

years     = np.sort(mue['year'].unique())
meter_ids = mue['meter_id'].nunique()

# Initialize plot
fig, axes = plt.subplots(len(years), 1, figsize = (15, 15), sharex = True)
fig.suptitle('Distribution of kWh, Normalized by Year & Meter ID', weight = 'bold', fontsize = 15)
fig.supylabel('Normalized Usage in Kilowatt Hours', weight = 'bold')

# Create subplots for each year
for i, year in enumerate(years):
    ax = sns.boxplot(data    = mue[mue['year'] == year],
                     x       = 'meter_id',
                     y       = 'kwh_normalized',
                     ax      = axes[i],
                     hue     = 'meter_id',
                     width   = 0.2)
    
    for flier in ax.lines[4::]:
        flier.set(markerfacecolor = '1')
    
    axes[i].set_title(year)
    axes[i].set_xlabel('Meter ID')
    axes[i].set_ylabel(' ')

# Rotate x-axis labels
plt.xticks(rotation = 90)
plt.tight_layout()
plt.subplots_adjust(top = 0.93)
plt.savefig("fig/eda/distribution_of_kwh.png")
plt.show()