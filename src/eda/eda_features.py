from utils import meter_usage

import pandas as pd

def feature_engineering(df : pd.DataFrame) -> pd.DataFrame:
    '''
    This function enriches the original DataFrame with new features to aid in further analysis and modeling.
    It addresses three main areas: time-based features, statistical normalization, and outlier identification.
    
    Methodology:
        1. Datetime Conversion and Time-Based Features:
            - 'interval_end_datetime' is converted to datetime type.
            - Extract 'year', 'month', 'month_name', and 'hour' as separate columns for easier time-based analysis.
        
        2. Statistical Normalization:
            - Normalize 'kwh' usage based on each 'meter_id'.
            - This helps in comparing kwh usage across different meters irrespective of their original scale.
        
        3. Outlier Identification:
            - A new boolean column 'extreme_outlier' identifies whether a record has extreme kwh usage.
            - Extreme outliers are those with normalized 'kwh' greater than 3 or less than -3.

        4. Period Classification:
            - A new string column 'period' classifies the time of the day into three categories: Off-peak, Mid-peak, and On-peak.
    
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
    df['hour']       = df['interval_end_datetime'].dt.hour

    # Normalize 'kwh' by 'meter_id'
    grouped              = df.groupby('meter_id').agg({'kwh': ['mean', 'std']}).reset_index()
    grouped.columns      = ['meter_id', 'mean_kwh', 'std_kwh']
    df                   = pd.merge(df, grouped, on = 'meter_id', how = 'left')
    df['kwh_normalized'] = (df['kwh'] - df['mean_kwh']) / df['std_kwh']
    
    # Identify extreme outliers
    df['extreme_outlier'] = df['kwh_normalized'].abs() > 3

    # Classify hour into periods
    df['period'] = df['hour'].apply(
        lambda hour: 
            'Off-peak: 12AM to 7AM' if 0 <= hour < 7 else (
            'Mid-peak: 7AM to 5PM, 9PM to 11PM' if (7 <= hour < 17) or (21 <= hour < 23) else 
            'On-peak: 5PM to 9PM'))
    
    return df

meter_usage_engineered = feature_engineering(meter_usage)