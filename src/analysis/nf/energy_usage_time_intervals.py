'''
    Final Project
    DS 5110 Intro to Data Management
    10/29/2023

    This file contains a fucntion that will create a figure displaying the total energy usage by
    Austin Street Brewery over the duration of the dataset.
'''
from utils.runtime import connect_to_db
import pandas as pd
import matplotlib.pyplot as plt
from utils.runtime import setup_plot_params

#####################################################################################################
##################################### usage_fig fucntion ############################################
#####################################################################################################
def usage_fig(df: pd.DataFrame) -> None:
    ''' 
        Function: usage_fig
        Parameteres: 1 pd.DataFrame
        Returns: None
    '''
    period = ['M', 'W', 'D']

    fig, axes = plt.subplots(3, 1, figsize = (15, 15))
    fig.suptitle('Energy Usage (kWh) Summary: \n ~ Day, Week, & Month', weight = 'bold', style = 'italic', fontsize = 24, y = 1)
    fig.supxlabel('Year-Month', weight = 'bold', fontsize = 16)
    fig.supylabel('Total Energy Usage (kWh)', weight = 'bold', fontsize = 16)
    for i, j in enumerate(period):

        df[j] = df['date'].dt.to_period(j)
        result = df.groupby(j)['kwh'].sum().reset_index()

        # create fig
        ax = axes[i]
        ax.plot_date(result[j], result['kwh'], '-.')

        # vline for ampion start
        ax.axvline(pd.to_datetime('2022-10'), color = 'red', linestyle='--', label = 'Solar Power Supply Start')

        if i == 0:
        # annotate
            for index, row in result.iterrows():
                ax.annotate(f"{row['kwh']:.2f}", (row[j], row['kwh']), textcoords="offset points", xytext=(0,5), ha='center', fontsize=7)
            ax.set_ylim(6000, 16000)

        ax.set_xlabel(None)
        ax.set_ylabel(None)
        ax.legend()
        ax.grid(True)
    axes[0].set_title('Monthly', weight = 'bold', style = 'italic')
    axes[1].set_title('Weekly', weight = 'bold', style = 'italic')
    axes[2].set_title('Daily', weight = 'bold', style = 'italic')

    save_path = ('fig/analysis/nf/usage_by_time_interval.png')
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

# create fig
usage_fig(df)