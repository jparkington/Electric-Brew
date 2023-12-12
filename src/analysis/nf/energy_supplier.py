'''
    Final Project
    DS 5110 Intro to Data Management
    10/29/2023

    This file contains a function that will create a figure that displays the kind of energy
    used by Austin Street; solar v. conventional
'''
from utils.runtime import connect_to_db
import pandas as pd
import matplotlib.pyplot as plt
from utils.runtime import setup_plot_params

import logging

# Set logging level for matplotlib to WARNING to suppress INFO messages
logging.getLogger('matplotlib').setLevel(logging.WARNING)

#####################################################################################################
##################################### supplier fig function #########################################
#####################################################################################################
def generate_supplier_fig(usage_df: pd.DataFrame, energy_percent_df: pd.DataFrame) -> None:
    '''
        Function: generate_supplier_fig
        Parameters: 2 pd.DataFrames
        Returns: None
    '''
    # create fig
    fig, axes = plt.subplots(2, 1, figsize = (15, 10))
    fig.suptitle('Energy Supply by Generation Type', weight = 'bold', fontsize = 20, y = 1)

    # line plot
    ax = axes[0]
    ax.plot_date(usage_df.index, 
                usage_df['Solar'], 
                '-o', 
                label = 'Solar',
                color = 'darkorange', 
                alpha = .5)
    ax.plot_date(usage_df.index, 
                usage_df['conventional_supplier'], 
                '-o',
                color = 'b',
                label = 'Conventional', 
                alpha = .5)

    # annotatation
    for index, row in usage_df.iterrows():
        if not pd.isnull(row['Solar']):
            ax.annotate(f"{row['Solar']:.2f}", 
                        (index, row['Solar']), 
                        textcoords = 'offset points', 
                        xytext = (0, 5), 
                        ha = 'center', 
                        fontsize = 7)

    for index, row in usage_df.iterrows():
        if not pd.isnull(row['conventional_supplier']):
            ax.annotate(f"{row['conventional_supplier']:.2f}", 
                        (index, row['conventional_supplier']), 
                        textcoords = 'offset points', 
                        xytext = (0, 5), 
                        ha = 'center', 
                        fontsize = 7)

    # labels & ticks
    ax.set_xticks(usage_df.index, usage_df.index.strftime('%B'), rotation = 45, weight = 'bold')
    ax.set_ylim(0, 14000)
    ax.set_xlabel(None)
    ax.set_ylabel('Energy Supplied (kwh)', weight = 'bold')
    ax.set_title('Total (kwh) Supplied by Energy Generation Type', weight = 'bold', style = 'italic')
    ax.legend(loc = 'upper left', shadow = True)
    ax.grid(True)


    # bar plot
    ax = axes[1]

    # bar 1
    bar1 = ax.bar(energy_percent_df.index, 
                energy_percent_df.solar_percent, 
                label = 'Solar',
                color = 'darkorange',
                width = .5,
                alpha = .5)

    # bar 2
    bar2 = ax.bar(energy_percent_df.index, 
                energy_percent_df.conventional_percent, 
                bottom = energy_percent_df.solar_percent, 
                label = 'Conventional',
                color = 'b',
                width = .5,
                alpha = .5)

    # annotations
    for bars in [bar1, bar2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + height / 2,
                    f'{height:.1f}%', ha = 'center', va = 'center', color = 'white', fontsize=7)

    # labels & ticks
    ax.set_xlabel(None)
    ax.set_ylabel('Percent of Energy Supplied', weight = 'bold')
    ax.set_title('Percent Energy Supplied by Generation Type', weight = 'bold', style = 'italic')
    ax.set_xticks(energy_percent_df.index, energy_percent_df.index, weight = 'bold', rotation = 45)
    ax.legend(shadow = True)
    plt.tight_layout()

    save_path = ('fig/analysis/nf/supplier_fig.png')
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
                   kwh
            FROM fct_electric_brew fe
                LEFT JOIN dim_datetimes dd ON fe.dim_datetimes_id = dd.id
                LEFT JOIN dim_bills db ON fe.dim_bills_id = db.id
            WHERE date >= '2022-09-01' AND date <= '2023-07-31'
        """

# engineer query # 1
usage_df = electric_brew.query(query).to_df()
usage_df['month'] = usage_df['date'].dt.to_period('M') 
usage_df['supplier'] = usage_df['supplier'].apply(lambda x: 'Solar' if x == 'Ampion' else 'conventional_supplier')
usage_df = usage_df.groupby(['month', 'supplier'])['kwh'].sum().unstack(fill_value=0)

# query # 2
query = """ SELECT supplier,
                   date,
                   kwh,
                   total_cost
            FROM fct_electric_brew fe
                LEFT JOIN dim_datetimes     dd ON fe.dim_datetimes_id = dd.id
                LEFT JOIN dim_bills         db ON fe.dim_bills_id     = db.id
            WHERE dd.date  >= '2022-09-01' AND dd.date <= '2023-07-31';
        """

# engineer query # 2
energy_percent_df = electric_brew.query(query).to_df()
energy_percent_df['month'] = energy_percent_df['date'].dt.month
energy_percent_df['energy_type'] = energy_percent_df['supplier'].apply(lambda x: 'Solar' if x == 'Ampion' else 'conventional_supplier')
energy_percent_df.sort_values('date', inplace = True)
energy_percent_df = energy_percent_df.groupby(['month', 'energy_type'], sort = False)['kwh'].sum().unstack(fill_value = 0)
energy_percent_df['total_kwh'] = energy_percent_df.Solar + energy_percent_df.conventional_supplier
energy_percent_df['solar_percent'] = (energy_percent_df.Solar/energy_percent_df.total_kwh) * 100
energy_percent_df['conventional_percent'] = (energy_percent_df.conventional_supplier/energy_percent_df.total_kwh) * 100
energy_percent_df.reset_index(inplace = True)
energy_percent_df.set_index('month', inplace = True)
month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
energy_percent_df.index = energy_percent_df.index.map(month_names)

# invoke function, generate fig
generate_supplier_fig(usage_df, energy_percent_df)