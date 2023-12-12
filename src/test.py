from utils.runtime import connect_to_db

def print_solar_projection_data():
    '''
    Prints statistics and samples for solar projection data.

    Outputs:
        - Summary statistics for actual and projected energy costs.
        - Cost per kWh for solar energy.
        - Sample data showing percentage difference between actual and projected costs.
    '''

    # Connect to the database
    electric_brew = connect_to_db()

    # Query for actual and projected costs
    query = """ SELECT supplier, date, kwh, total_cost
                FROM fct_electric_brew
                LEFT JOIN dim_datetimes ON fct_electric_brew.dim_datetimes_id = dim_datetimes.id
                LEFT JOIN dim_bills ON fct_electric_brew.dim_bills_id = dim_bills.id
                WHERE date >= '2022-09-01' AND date <= '2023-07-31';
             """
    cost_df = electric_brew.query(query).to_df()
    cost_df['month'] = cost_df['date'].dt.to_period('M')
    cost_df.sort_values('date', inplace=True)
    cost_df['energy_type'] = cost_df['supplier'].apply(lambda x: 'Solar' if x == 'Ampion' else 'conventional_supplier')
    grouped_cost_df = cost_df.groupby(['month', 'energy_type']).agg({'total_cost': 'sum', 'kwh': 'sum'}).unstack(fill_value=0)
    grouped_cost_df['solar_cost_per_kwh'] = grouped_cost_df.total_cost.Solar / grouped_cost_df.kwh.Solar
    grouped_cost_df['total_kwh'] = grouped_cost_df.kwh.Solar + grouped_cost_df.kwh.conventional_supplier
    grouped_cost_df['total_cost_2'] = grouped_cost_df.total_cost.Solar + grouped_cost_df.total_cost.conventional_supplier
    grouped_cost_df['projected_costs'] = grouped_cost_df.solar_cost_per_kwh * grouped_cost_df.total_kwh

    # Print summary statistics for actual and projected costs
    print("Actual vs. Projected Energy Costs:")
    print(grouped_cost_df[['total_cost_2', 'projected_costs']].describe())

    # Print cost per kWh for solar energy
    print("\nSolar Cost per kWh:")
    print(grouped_cost_df['solar_cost_per_kwh'].describe())

    # Print sample data for percentage difference
    percent_diff = ((grouped_cost_df['projected_costs'] - grouped_cost_df['total_cost_2']) / grouped_cost_df['total_cost_2']) * 100
    print("\nPercentage Difference Between Actual and Projected Costs:")
    print(percent_diff)

if __name__ == "__main__":
    print_solar_projection_data()
