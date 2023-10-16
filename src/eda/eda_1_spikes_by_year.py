from eda_features import meter_usage_engineered as mue
from utils import set_plot_params

import numpy             as np
import matplotlib.pyplot as plt
import seaborn           as sns

colors = set_plot_params() # Enables dark mode, larger canvas sizes, and sizing considerations

# Filter the DataFrame for extreme outliers
plot_df = mue.loc[mue.extreme_outlier == True]
unique_years = np.sort(plot_df['year'].unique())[::-1]

# Initialize plot
fig, axes = plt.subplots(len(unique_years), 1, sharex = True)
fig.suptitle('Count of Energy Spikes by Meter ID & Year', weight = 'bold', fontsize = 15)

# Create subplots for each year
for i, year in enumerate(unique_years):
    sns.countplot(data    = plot_df[plot_df['year'] == year], 
                  x       = 'extreme_outlier', 
                  ax      = axes[i], 
                  hue     = 'meter_id')
    
    axes[i].set_title(year)
    axes[i].set_xlabel('Extreme Outliers')
    axes[i].set_ylabel(None)
    
    if i == 0:
        leg = axes[i].legend(title    = 'Meter IDs', 
                             ncols    = 1, 
                             fancybox = True, 
                             shadow   = True)
    else:
        axes[i].get_legend().remove()

plt.xticks([])
plt.tight_layout()
plt.subplots_adjust(top = 0.90)
plt.savefig("fig/eda/spikes_by_year.png")
plt.show()
