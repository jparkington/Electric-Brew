import pandas as pd
from utils.runtime import connect_to_db

def print_operational_area_usage_stats():
    '''
    Prints statistics for total kWh usage by operational area over time.

    Outputs:
        - Summary statistics by operational area.
        - Sample data showing year-month and kWh usage.
    '''

    fct_electric_brew = connect_to_db()
    query = """
    SELECT dm.meter_id, dd.year, dd.month, dm.operational_area,
           SUM(kwh) AS total_kWh_usage, SUM(total_cost) AS total_cost
    FROM fct_electric_brew fct   
    JOIN dim_meters dm ON fct.dim_meters_id = dm.id
    JOIN dim_datetimes dd ON fct.dim_datetimes_id = dd.id
    GROUP BY dm.meter_id, dd.year, dd.month, dm.operational_area
    ORDER BY dm.meter_id, dd.year, dd.month, dm.operational_area
    """
    df = fct_electric_brew.execute(query).fetch_df()

    # Preprocessing the data
    df['year_month'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))
    pivot_df = df.pivot_table(index='year_month', columns='operational_area', values='total_kWh_usage')

    # Print basic statistics for each operational area
    print("Basic Statistics for Each Operational Area:")
    for area in pivot_df.columns:
        print(f"\n{area}:")
        print(pivot_df[area].describe())

    # Print a sample of the pivoted data
    print("\nSample of Pivoted Data:")
    print(pivot_df)

if __name__ == "__main__":
    print_operational_area_usage_stats()
