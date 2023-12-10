import calendar
import matplotlib.pyplot as plt
import pandas  as pd
import seaborn as sns

from analysis.jp.flat import prepare_data
from utils.runtime    import find_project_root

def eda2(df: pd.DataFrame):
    '''
    Plots a heatmap to visualize the hourly variation of kWh usage by month.

    Methodology:
        1. Pivot the dataframe to get the average kWh used per hour for each month.
        2. Use Seaborn's heatmap to visualize this data.
        3. Relabel the x-axis to use month names instead of numbers

    Parameters:
        df (pd.DataFrame): The dataframe containing 'hour', 'month', and 'kwh' columns.

    Produces:
        A heatmap saved as a PNG file and displayed on the screen.
    '''

    # 1: Pivoting the data for the heatmap
    hourly_kwh_by_month = df.pivot_table(index   = 'hour', 
                                         columns = 'month', 
                                         values  = 'kwh', 
                                         aggfunc = 'mean')

    # 2: Creating the heatmap
    p = sns.heatmap(hourly_kwh_by_month, 
                    cmap     = 'cividis',
                    cbar_kws = {'label': 'Avg. kWh Used'})

    # 3: Setting month names as labels
    month_names = [calendar.month_name[i] for i in range(1, 13)]
    p.set_xticklabels(month_names)

    # Final plot settings
    plt.grid(False)
    plt.xlabel('Month')
    plt.ylabel('Hour of Day')
    plt.title('$02$: Hourly Variation of kWh by Month')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/02 - Hourly Variation of kWh by Month.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    df = prepare_data()
    eda2(df)
