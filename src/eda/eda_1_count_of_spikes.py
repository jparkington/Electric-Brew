from eda_features  import meter_usage_engineered as mue
from utils.runtime import setup_plot_params

import matplotlib.pyplot as plt
import seaborn as sns  

colors = setup_plot_params() # Enables dark mode, larger canvas sizes, and sizing considerations

# Filter data for extreme outliers
plot_df = mue.loc[mue.extreme_outlier == True]

# Create countplot
sns.countplot(data = plot_df, 
              x    = 'extreme_outlier', 
              hue  = 'meter_id')

plt.title('Count of Energy Spikes by Meter ID')
plt.xlabel('Extreme Outlier')
plt.ylabel(None)
plt.legend(title = 'Meter ID')

plt.xticks([])
plt.tight_layout()
plt.savefig("fig/eda/count_of_spikes.png")
plt.show()
