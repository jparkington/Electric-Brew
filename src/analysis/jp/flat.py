import numpy  as np
import pandas as pd

from utils.dataframes import fct_electric_brew, dim_datetimes, dim_meters, dim_bills
from utils.runtime    import setup_plot_params

def prepare_data() -> pd.DataFrame:
    '''
    Prepares and returns a flat, slightly engineered dataframe by merging 'fct_electric_brew' with dimension tables.

    Methodology:
        1. Merge 'fct_electric_brew' with 'dim_datetimes', 'dim_meters', and 'dim_bills'.
        2. Handle missing values in the 'supplier' column by replacing them with 'Unspecified'.

    Returns:
        pd.DataFrame: The prepared dataframe.
    '''

    setup_plot_params() # Setting up consistent plotting colors and sizes for all subsequent scripts

    # 1: Merging the fact and dimension tables
    df = fct_electric_brew.merge(dim_datetimes, left_on = 'dim_datetimes_id', right_on = 'id', suffixes = ('', '_dd')) \
                          .merge(dim_meters,    left_on = 'dim_meters_id',    right_on = 'id', suffixes = ('', '_dm')) \
                          .merge(dim_bills,     left_on = 'dim_bills_id',     right_on = 'id', suffixes = ('', '_db'))

    # 2: Handling missing values in 'supplier'
    df['supplier'] = df['supplier'].replace([np.nan, ''], 'Unspecified')

    return df

if __name__ == "__main__":

    df: pd.DataFrame = prepare_data()