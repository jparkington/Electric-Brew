import pandas as pd
from analysis.jp.flat import prepared_data

def print_heatmap_data_details(df: pd.DataFrame = prepared_data):
    '''
    Prints details for the heatmap data used in the eda2 function.

    Outputs:
        - Basic statistics for each month.
        - Sample of the pivoted table for the heatmap.
    '''

    # Pivoting the data for the heatmap
    hourly_kwh_by_month = df.pivot_table(index   = 'hour', 
                                         columns = 'month', 
                                         values  = 'kwh', 
                                         aggfunc = 'mean')

    # Print basic statistics for each month
    print("Basic Statistics for Each Month:")
    for month in range(1, 13):
        month_data = df[df['month'] == month]['kwh']
        print(f"\nMonth {month} - Avg: {month_data.mean():.2f}, Min: {month_data.min():.2f}, Max: {month_data.max():.2f}, Std: {month_data.std():.2f}")

    # Print a sample of the pivoted table
    print("\nSample of the Pivoted Table for the Heatmap:")
    print(hourly_kwh_by_month)  # Adjust the number based on your dataset size

if __name__ == "__main__":
    print_heatmap_data_details()
