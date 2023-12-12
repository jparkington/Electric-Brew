import pandas as pd
from analysis.jp.flat import prepared_data

def print_eda3_data_details(df: pd.DataFrame = prepared_data):
    '''
    Prints details for the data used in the eda3 function.

    Outputs:
        - Basic statistics for 'total_cost' grouped by 'period'.
        - Sample of the grouped data for visual inspection.
    '''

    # Grouping the data and calculating mean total_cost
    dfg = df.groupby(['period', 'date'])['total_cost'].mean().reset_index()

    # Print basic statistics for each period
    print("Basic Statistics for Each Period:")
    period_stats = dfg.groupby('period')['total_cost'].describe()
    print(period_stats)

    # Print a sample of the grouped data
    print("\nSample of the Grouped Data:")
    print(dfg.sample(5))  # Adjust the number based on your dataset size

if __name__ == "__main__":
    print_eda3_data_details()
