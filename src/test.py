import pandas as pd
from analysis.jp.jp06 import lasso_outputs

def print_lasso_analysis_details(lasso_data: dict = lasso_outputs):
    '''
    Prints detailed information about the features selected by the LASSO model.

    Outputs:
        - List of selected features and their corresponding LASSO coefficients.
        - Basic statistics of the LASSO coefficients.
    '''

    # Extracting the feature importance data
    ft_importance = lasso_data['ft_importance']

    # Print the selected features and their coefficients
    print("Selected Features and LASSO Coefficients:")
    print(ft_importance)

    # Print basic statistics of the coefficients
    print("\nLASSO Coefficients Statistics:")
    print(ft_importance.describe())

if __name__ == "__main__":
    print_lasso_analysis_details()
