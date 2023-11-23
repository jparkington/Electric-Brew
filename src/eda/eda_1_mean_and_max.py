from eda_features  import meter_usage_engineered as mue
from utils.runtime import set_plot_params

import logging
import numpy             as np
import matplotlib.pyplot as plt
import pandas            as pd
import seaborn           as sns

colors = set_plot_params() # Enables dark mode, larger canvas sizes, and sizing considerations

logger = logging.getLogger('matplotlib') # Get the logger for 'matplotlib'
logger.setLevel(logging.WARN)            # Set the logging level to WARN to ignore INFO messages

# Generate and modify grouped DataFrame
max_mean_dif_df = mue.groupby(['month', 
                               'year', 
                               'meter_id']).agg({'kwh': ['max', 
                                                         'mean', 
                                                         'median'],
                                                 'month_name': 'first'}).reset_index()

max_mean_dif_df['max_mean_diff'] = (((max_mean_dif_df.kwh['max'] - max_mean_dif_df.kwh['mean'])/max_mean_dif_df.kwh['mean']) * 100).round(2)
max_mean_dif_df.columns = max_mean_dif_df.columns.droplevel(level = 1)
max_mean_dif_df.columns = ['month', 'year', 'meter_id', 'max_usage', 'mean_usage', 'median_usage', 'month_name', 'max_mean_diff']
    
plot_params = {
    'max_usage_plot': 
        {'y'    : 'max_usage',
        'title' : 'Max Usage Per 15-Minute Interval By Meter, Month & Year'},
    'mean_usage_plot': {
        'y'     : 'mean_usage',
        'title' : 'Mean Usage Per 15-Minute Interval By Meter, Month & Year'},
    'max_mean_diff_plot': {
        'y'     : 'max_mean_diff',
        'title' : 'Percent Difference Between Mean & Max Usage By Meter & Year'}
}

def generate_usage_plot(df      : pd.DataFrame, 
                        x_col   : str, 
                        y_col   : str, 
                        hue_col : str, 
                        title   : str):
    '''
    Generates a scatter plot to visualize usage by meter over time.
    
    Methodology:
        1. Identify unique years in the DataFrame for subplots.
        2. Create subplots for each unique year.
        3. Plot scatter plots in each subplot, coloring by the hue column.
        4. Customize plot titles, axis labels, and legend.
    
    Parameters:
        df      (DataFrame)  : The DataFrame containing the data to be plotted.
        x_col   (str)        : The column name to be used for the x-axis.
        y_col   (str)        : The column name to be used for the y-axis.
        hue_col (str)        : The column name to be used for hue.
        title   (str)        : The title of the main plot.
    '''

    # Identify unique years for subplots
    unique_years = np.sort(df['year'].unique())[::-1]
    
    # Create subplots
    fig, axes = plt.subplots(len(unique_years), 1, sharex = True)
    
    # Customize main plot titles and axis labels
    fig.suptitle(title, weight = 'bold', fontsize = 15)
    fig.supylabel('Usage in Kilowatt Hours', weight = 'bold')
    
    # Generate scatter plots for each year
    for i, year in enumerate(unique_years):
        sns.scatterplot(data    = df[df['year'] == year], 
                        x       = x_col, 
                        y       = y_col, 
                        ax      = axes[i], 
                        hue     = hue_col)
        
        axes[i].set_title(year)
        axes[i].set_xlabel('Month')
        axes[i].set_ylabel(' ')

        if i == 0:
            leg = axes[i].legend(title    = 'Meter IDs', 
                                 ncols    = 1, 
                                 fancybox = True, 
                                 shadow   = True)
        else:
            axes[i].get_legend().remove()

    plt.xticks(rotation = 90)
    plt.tight_layout()
    plt.subplots_adjust(top = 0.90)
    plt.savefig(f"fig/eda/{y_col}.png")
    plt.show()

for k, v in plot_params.items():
    generate_usage_plot(max_mean_dif_df, 
                        'month_name', 
                        v['y'], 
                        'meter_id', 
                        v['title'])
