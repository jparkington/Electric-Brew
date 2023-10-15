from utils import meter_usage

import logging
import numpy             as np
import matplotlib.pyplot as plt
import pandas            as pd
import seaborn           as sns

logger = logging.getLogger('matplotlib') # Get the logger for 'matplotlib'
logger.setLevel(logging.WARN)            # Set the logging level to WARN to ignore INFO messages

def feature_engineering(df : pd.DataFrame) -> pd.DataFrame:
    '''
    This function enriches the original DataFrame with new features to aid in further analysis and modeling.
    It addresses three main areas: time-based features, statistical normalization, and outlier identification.
    
    Methodology:
        1. Datetime Conversion and Time-Based Features:
            - 'interval_end_datetime' is converted to datetime type.
            - Extract 'year', 'month', and 'month_name' as separate columns for easier time-based analysis.
        
        2. Statistical Normalization:
            - Normalize 'kwh' usage based on each 'meter_id'.
            - This helps in comparing kwh usage across different meters irrespective of their original scale.
        
        3. Outlier Identification:
            - A new boolean column 'extreme_outlier' identifies whether a record has extreme kwh usage.
            - Extreme outliers are those with normalized 'kwh' greater than 3 or less than -3.
    
    Parameters:
        df (pd.DataFrame): The original DataFrame containing meter usage data.
    
    Returns:
        df (pd.DataFrame): The DataFrame after feature engineering.
    '''

    # Convert to datetime and extract year, month, and month_name
    df['interval_end_datetime'] = pd.to_datetime(df['interval_end_datetime'], format = '%m/%d/%Y %I:%M:%S %p')
    df['year']       = df['interval_end_datetime'].dt.year
    df['month']      = df['interval_end_datetime'].dt.month
    df['month_name'] = df['interval_end_datetime'].dt.strftime('%B')

    # Normalize 'kwh' by 'meter_id'
    grouped = df.groupby('meter_id').agg({'kwh': ['mean', 'std']}).reset_index()
    grouped.columns = ['meter_id', 'mean_kwh', 'std_kwh']
    df = pd.merge(df, grouped, on='meter_id', how='left')
    df['kwh_normalized'] = (df['kwh'] - df['mean_kwh']) / df['std_kwh']
    
    # Identify extreme outliers
    df['extreme_outlier'] = df['kwh_normalized'].abs() > 3
    
    return df

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
            plt.setp(leg.get_title(), weight = 'bold')
        else:
            axes[i].get_legend().remove()

    plt.xticks(rotation = 90)
    plt.tight_layout()
    plt.subplots_adjust(top = 0.90)
    plt.savefig(f"fig/eda/{y_col}.png")
    plt.show()

meter_usage_engineered = feature_engineering(meter_usage)