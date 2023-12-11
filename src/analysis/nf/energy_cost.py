'''
    Final Project
    DS 5110 Intro to Data Management
    10/29/2023

    This file contains a function that will create a figure displaying Austin Street's
    energy costs.
'''
from utils.runtime import connect_to_db
import pandas as pd
import matplotlib.pyplot as plt
from utils.runtime import setup_plot_params

#####################################################################################################################
##################################### generate cost function ########################################################
#####################################################################################################################

def generate_cost_fig(total_cost_df: pd.DataFrame, cost_df: pd.DataFrame, kwh_df: pd.DataFrame) -> None:
    # create fig
    fig, axes = plt.subplots(3, 1, figsize = (15, 15))
    fig.suptitle('Energy Costs Summary', weight = 'bold', fontsize = 24, y = 1)

    # Plot 1 
    ax = axes[0]
    ax.plot_date(total_cost_df.index, total_cost_df.total_cost, '-o', color = 'b')
    ax.set_xticks(total_cost_df.index, total_cost_df.index, rotation = 90)

    # vline for ampion start
    ax.axvline(pd.to_datetime('2022-10'), color = 'red', linestyle = '--', label = 'Solar Power Supply Start')


    # annotate
    for index, row in total_cost_df.iterrows():
        x_val = row.name.to_timestamp().to_pydatetime()
        ax.annotate(f"${row['total_cost']:.2f}", 
                    (x_val, row['total_cost']), 
                    textcoords = "offset points", 
                    xytext = (0,5), 
                    ha = 'center', 
                    fontsize = 7)

    ax.set_xlabel('Year-Month', weight = 'bold')
    ax.set_ylabel('Total Energy Cost ($)', weight = 'bold')
    ax.set_title('Total Energy Cost ($): Month', weight = 'bold', style = 'italic', fontsize = 14)
    ax.legend(shadow = True)
    ax.grid(True)

    # Plot 2 
    ax = axes[1]
    ax.plot_date(cost_df.index, 
                cost_df.total_cost.Solar, 
                '-o',
                color = 'darkorange', 
                label = 'Solar', 
                alpha = .5)
    ax.plot_date(cost_df.index, 
                cost_df.total_cost.conventional_supplier, 
                '-o',
                color = 'b', 
                label = 'Conventional Supplier', 
                alpha = .5)

    # annotation
    for index, row in cost_df.iterrows():

    # manually set the x_val
        x_val = pd.to_datetime(str(index.year) + '-' + str(index.month) + '-28')
        
        # pull out individual total_cost values for each energy type within the month
        solar_cost = row['total_cost']['Solar']
        conv_supplier_cost = row['total_cost']['conventional_supplier']
        
        # set annotation strings
        cost_str_solar = f'${solar_cost:.2f}'
        cost_str_conv = f'${conv_supplier_cost:.2f}'

        # annotate plot
        ax.annotate(cost_str_solar, (x_val, solar_cost), textcoords = "offset points", xytext = (0, 5),
                    ha = 'center', va = 'bottom', fontsize = 7)
        ax.annotate(cost_str_conv, (x_val, conv_supplier_cost), textcoords = "offset points", xytext = (0, 5),
                    ha = 'center', va = 'bottom', fontsize = 7)
        
    ax.set_title('Total Energy Cost ($): By Generation Type', weight = 'bold', style = 'italic', fontsize = 14)
    ax.set_xlabel('Year-Month', weight = 'bold')
    ax.set_ylabel('Total Energy Cost ($)', weight = 'bold')
    ax.set_xticks(cost_df.index, cost_df.index, rotation = 90)
    ax.legend(shadow = True)
    ax.grid(True)

    # Plot 3 
    ax = axes[2]
    ax.plot_date(kwh_df.month, kwh_df.Solar, '-o', color = 'darkorange', label = 'Solar', alpha = .5)
    ax.plot_date(kwh_df.month, kwh_df.conventional_supplier, '-o', color = 'b', label = 'Conventional', alpha = .5)

    # annotate
    for x, y in zip(kwh_df.month, kwh_df.conventional_supplier):
        ax.annotate(f"${y:.2f}", (x, y), textcoords="offset points", xytext=(0, 5),
                    ha='center', fontsize=7)
    for x, y in zip(kwh_df.month, kwh_df.Solar):
        ax.annotate(f"${y:.2f}", (x, y), textcoords="offset points", xytext=(0, 5),
                    ha='center', fontsize=7)
        
    # add hline for solar price
    ax.axhline(y = 0.18, color='red', linestyle='--', label='Legislated Solar Price')

    # labels and ticks
    ax.set_xticks(kwh_df.month, kwh_df.month, rotation = 90)
    ax.set_title('Cost Per kWh: By Generation Type', weight = 'bold', style = 'italic', fontsize = 14)
    ax.set_xlabel('Year-Month', weight = 'bold')
    ax.set_ylabel('Cost Per kWh ($)', weight = 'bold')
    ax.legend()
    plt.tight_layout()

    save_path = ('fig/analysis/nf/aggregated_costs_fig.png')
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
            WHERE dd.date <= '2023-07-31';
        """
cost_df = electric_brew.query(query).to_df()

# engineer query # 1
cost_df['month'] = cost_df['date'].dt.to_period('M') 
cost_df.sort_values('date', inplace = True)
cost_df = cost_df.loc[cost_df.total_cost != 0]

# create grouped_df for total cost by month
total_cost_df = cost_df.groupby('month')['total_cost'].sum().round(2).to_frame()

# query # 2
query = """ SELECT supplier,
                   date,
                   kwh,
                   total_cost,
            FROM fct_electric_brew fe
                LEFT JOIN dim_datetimes     dd ON fe.dim_datetimes_id = dd.id
                LEFT JOIN dim_bills         db ON fe.dim_bills_id     = db.id
            WHERE dd.date  >= '2022-09-01' AND dd.date <= '2023-07-31';
        """

# engineer query # 2
cost_df = electric_brew.query(query).to_df()
cost_df['month'] = cost_df['date'].dt.to_period('M') 
cost_df.sort_values('date', inplace = True)
cost_df['energy_type'] = cost_df['supplier'].apply(lambda x: 'Solar' if x == 'Ampion' else 'conventional_supplier')
cost_df = cost_df.groupby(['month', 'energy_type'], sort = False).agg({'total_cost': 'sum',
                                                                       'kwh': 'sum'}).unstack(fill_value = 0)

# engineer data for plot # 3
kwh_df = cost_df.total_cost/cost_df.kwh
kwh_df.reset_index(inplace= True)
kwh_df

# invoke function gerneate figure
generate_cost_fig(total_cost_df, cost_df, kwh_df)