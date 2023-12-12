'''
    Final Project
    DS 5110 Intro to Data Management
    10/29/2023

    This file contains a fucntion that will create a figure displaying the total energy usage by
    Austin Street Brewery over the duration of the dataset.
'''
from utils.runtime import connect_to_db
from utils.runtime import setup_plot_params
import pandas as pd
import matplotlib.pyplot as plt 

#####################################################################################################
##################################### usage_fig_b fucntion ##########################################
#####################################################################################################
def usage_fig_b(df: pd.DataFrame) -> None:
    ''' 
        Function: usage_fig
        Parameteres: 1 pd.DataFrame
        Returns: None
    '''
    # create fig
    plt.plot_date(df['month'], df['kwh'], '-o')

    # vline for ampion start
    plt.axvline(pd.to_datetime('2022-10'), color = 'red', linestyle='--', label = 'Solar Power Supply Start')

    # set xticks
    plt.xticks(df['month'], df['month'], rotation = 90)

    # annotate
    for index, row in df.iterrows():
        plt.annotate(f"{row['kwh']:.2f}", (row['month'], row['kwh']), textcoords="offset points", xytext=(0,5), ha='center', fontsize=7)

    plt.xlabel('Year-Month', weight = 'bold')
    plt.ylabel('Total Energy Usage (kWh)', weight = 'bold')
    plt.title('Total Energy Usage (kWh)', weight = 'bold', style = 'italic', fontsize = 16)
    plt.legend(shadow = True)
    plt.grid(True)

    save_path = ('fig/analysis/nf/aggregated_usage_fig.png')
    plt.savefig(save_path, dpi = 300, bbox_inches = 'tight')
    plt.show()

#####################################################################################################
######################################## main #######################################################
#####################################################################################################
setup_plot_params()

# define query
query = """ SELECT date,
                   kwh
            FROM fct_electric_brew fe
            LEFT JOIN dim_datetimes dd ON fe.dim_datetimes_id = dd.id
        """

# connect to db
electric_brew = connect_to_db()

# execute query, save a df
df = electric_brew.query(query).to_df()

# engineer df
df['month'] = df['date'].dt.to_period('M')
df = df.groupby('month')['kwh'].sum().reset_index()
df['month_name'] = df['month'].dt.strftime('%B')

# create fig
usage_fig_b(df)