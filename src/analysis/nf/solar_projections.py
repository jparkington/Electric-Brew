'''
    Final Project
    DS 5110 Intro to Data Management
    10/29/2023

    This file will generate a figure that displays what Austin Street Brewery would have paid
    had all the power come from solar providers over the last year
'''
from utils.runtime import connect_to_db
import pandas as pd
import matplotlib.pyplot as plt
from utils.runtime import setup_plot_params
import datetime

#####################################################################################################
##################################### usage_fig fucntion ############################################
#####################################################################################################
def generate_solor_projections_fig(cost_df: pd.DataFrame) -> None:
    '''
        Function: generate_supplier_fig
        Parameters: 2 pd.DataFrames
        Returns: None
    '''
    # create fig
    plt.figure(figsize = (15, 7))

    # plot costs 
    plt.plot_date(cost_df.index, cost_df.total_cost_2, '-o', color = 'b', alpha = .7, label = 'Actual Costs')
    plt.plot_date(cost_df.index, cost_df.projected_costs, '-o', color = 'darkorange', label = 'Projected Costs ~ 100 % Solar')
    plt.xticks(cost_df.index, cost_df.index, rotation = 90)

    # annotate acutal costs
    for idx in range(len(cost_df)):
        cost_value = "${:.2f}".format(cost_df['total_cost_2'].iloc[idx])
        plt.annotate(cost_value,
                    xy=(cost_df.index[idx], cost_df['total_cost_2'].iloc[idx]),
                    xytext=(5, -10), 
                    textcoords='offset points',
                    ha='center',
                    fontsize=8,
                    color='white')

    # annotate projected costs
    for idx in range(len(cost_df)):
        projected_cost_value = "${:.2f}".format(cost_df['projected_costs'].iloc[idx])
        plt.annotate(projected_cost_value,
                    xy=(cost_df.index[idx], cost_df['projected_costs'].iloc[idx]),
                    xytext=(5, 5),  
                    textcoords='offset points',
                    ha='center',
                    fontsize=8,
                    color='white')

    # calculate percent difference
    percent_diff = (((cost_df['projected_costs'] - cost_df['total_cost_2']) / cost_df['total_cost_2']) * 100)

    # fill between the lines
    plt.fill_between(cost_df.index, cost_df.total_cost_2, cost_df.projected_costs, color='darkorange', alpha=0.3, label = 'Cost Difference')

    # annotate percent difference
    for idx, val in percent_diff.items():
        x_val = idx.to_timestamp().to_pydatetime()
        total_cost_val = cost_df.loc[idx, 'total_cost_2']
        projected_cost_val = cost_df.loc[idx, 'projected_costs']
        
        # Ensure that the values are single elements, not Series
        if isinstance(total_cost_val, pd.Series):
            total_cost_val = float(total_cost_val.iloc[0])
        if isinstance(projected_cost_val, pd.Series):
            projected_cost_val = float(projected_cost_val.iloc[0])

        y_val = (total_cost_val + projected_cost_val) / 2
        plt.annotate(f"{val:.2f}%",
                    (x_val, y_val),
                    textcoords="offset points",
                    xytext=(90, 0),
                    ha='center',
                    fontsize=8,
                    color='red',
                    weight = 'bold')
        
    # add dummy label to legend for annotations
    handles, labels = plt.gca().get_legend_handles_labels()
    extra_label = 'Percent Difference'
    extra_dummy, = plt.plot([], marker='.', linestyle = 'None', color = 'r', label=extra_label)
    handles.append(extra_dummy)
    labels.append(extra_label)

    # labels, legend, ticks, and lims
    plt.legend(handles=handles, labels=labels, loc='upper left', shadow = True)
    plt.xlabel('Year-Month', weight = 'bold')
    plt.ylabel('Energy Costs ($)', weight = 'bold')
    plt.title('Projected Costs v. Actual Costs \n ~ 100% Percent Solar', weight = 'bold', style = 'italic', fontsize = 14)
    plt.ylim(1500, 3750)
    start_date = datetime.datetime(2022, 8, 31)  
    end_date = datetime.datetime(2023, 8, 15)  
    plt.xlim(start_date, end_date)
    plt.grid(True)

    # save fig
    save_path = ('fig/analysis/nf/projections_fig.png')
    plt.savefig(save_path, dpi = 300, bbox_inches = 'tight')
    plt.show()

#####################################################################################################
######################################## main #######################################################
#####################################################################################################
setup_plot_params()

# connect to db
electric_brew = connect_to_db()

# query # 1
query = """ SELECT supplier,
                   date,
                   kwh,
                   total_cost,
            FROM fct_electric_brew fe
                LEFT JOIN dim_datetimes     dd ON fe.dim_datetimes_id = dd.id
                LEFT JOIN dim_bills         db ON fe.dim_bills_id     = db.id
            WHERE dd.date  >= '2022-09-01' AND dd.date <= '2023-07-31';
        """
cost_df = electric_brew.query(query).to_df()

# engineer query
cost_df['month'] = cost_df['date'].dt.to_period('M') 
cost_df.sort_values('date', inplace = True)
cost_df = cost_df.loc[cost_df.total_cost != 0]
cost_df['energy_type'] = cost_df['supplier'].apply(lambda x: 'Solar' if x == 'Ampion' else 'conventional_supplier')
cost_df = cost_df.groupby(['month', 'energy_type'], sort = False).agg({'total_cost': 'sum', 'kwh': 'sum'}).unstack(fill_value = 0)
cost_df['solar_cost_per_kwh'] = cost_df.total_cost.Solar/cost_df.kwh.Solar
cost_df['total_kwh'] = cost_df.kwh.Solar + cost_df.kwh.conventional_supplier
cost_df['total_cost_2'] = cost_df.total_cost.Solar + cost_df.total_cost.conventional_supplier
cost_df = cost_df[['solar_cost_per_kwh', 'total_kwh', 'total_cost_2']]
cost_df['projected_costs'] = cost_df.solar_cost_per_kwh * cost_df.total_kwh
cost_df = cost_df[['total_cost_2', 'projected_costs']]

# execute function, create fig
generate_solor_projections_fig(cost_df)