from eda_1_feature_engineering import meter_usage_engineered as mue
from utils import set_plot_params

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns  

colors = set_plot_params() # Enables dark mode, larger canvas sizes, and sizing considerations

years = np.sort(mue.year.unique())

fig, axes = plt.subplots(4, 1, figsize = (8, 10), sharex = True)
j = 0
fig.suptitle('Distribution of Kilowatt Hour Normalized \n By Year & Meter ID',
             weight = 'bold',
             fontsize = 16)
fig.supxlabel('Meter ID', 
              weight = 'bold')
fig.supylabel('Usage in Kilowatt Hours Normalized',
              weight = 'bold')
for i in years:
    sns.boxplot(data = mue.loc[mue.year == i], 
                 x = 'meter_id', 
                 y = 'kwh_normalized', 
                 ax = axes[j],
                 hue = 'meter_id',
                 color = 'firebrick')
    axes[j].set_title(years[j])
    axes[j].set_xlabel(None)
    axes[j].set_ylabel(None)
    axes[j].get_legend().remove()
    j += 1
plt.xticks(rotation = 45)