from utils import meter_usage

import numpy             as np
import matplotlib.pyplot as plt
import pandas            as pd
import seaborn           as sns

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
    grouped_df = df.groupby('meter_id').agg({'kwh': ['mean', 'std']}).reset_index()
    df = pd.merge(df, grouped_df, on = 'meter_id', how = 'left')
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
        1. Summarize the DataFrame by grouping by month, year, and meter_id.
        2. Calculate the difference between maximum and mean usage as a percentage.
        3. Identify unique years in the DataFrame for subplots.
        4. Create subplots for each unique year.
        5. Plot scatter plots in each subplot, coloring by the hue column.
        6. Customize plot titles, axis labels, and legend.
    
    Parameters:
        df      (DataFrame)  : The DataFrame containing the data to be plotted.
        x_col   (str)        : The column name to be used for the x-axis.
        y_col   (str)        : The column name to be used for the y-axis.
        hue_col (str)        : The column name to be used for hue.
        title   (str)        : The title of the main plot.
    '''

    # Summarize the DataFrame
    grouped_df = df.groupby(['month', 'year', 'meter_id']) \
                   .agg({'kwh'        : ['max', 'mean', 'median'],
                         'month_name' : 'first'}).reset_index()
    
    # Calculate max-mean difference as a percentage
    grouped_df['max_mean_diff'] = (((grouped_df['kwh']['max'] - grouped_df['kwh']['mean']) / grouped_df['kwh']['mean']) * 100).round(2)
    grouped_df.columns = grouped_df.columns.droplevel(level=1)
    grouped_df.columns = ['month', 'year', 'meter_id', 'max_usage', 'mean_usage', 'median_usage', 'month_name', 'max_mean_diff']
    
    # Identify unique years for subplots
    unique_years = np.sort(grouped_df['year'].unique())[::-1]
    
    # Create subplots
    fig, axes = plt.subplots(len(unique_years), 1, figsize=(8, 10), sharex=True)
    
    # Customize main plot titles and axis labels
    fig.suptitle(title, weight = 'bold', fontsize = 16)
    fig.supxlabel('Month', weight = 'bold')
    fig.supylabel('Usage in Kilowatt Hours', weight = 'bold')
    
    # Generate scatter plots for each year
    for i, year in enumerate(unique_years):
        sns.scatterplot(data    = grouped_df[grouped_df['year'] == year], 
                        x       = x_col, 
                        y       = y_col, 
                        ax      = axes[i], 
                        hue     = hue_col, 
                        palette = 'Paired')
        
        axes[i].set_title(year)
        axes[i].set_xlabel(None)
        axes[i].set_ylabel(None)
        
        # Remove legend for all but the last plot
        if i != len(unique_years) - 1:
            axes[i].get_legend().remove()
    
    plt.legend(title = 'Meter IDs', fancybox = True, shadow = True, bbox_to_anchor = (1.25, len(unique_years)))
    plt.xticks(rotation = 45)

meter_usage_engineered = feature_engineering(meter_usage)