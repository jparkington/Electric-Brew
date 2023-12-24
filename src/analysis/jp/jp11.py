import matplotlib.pyplot as plt
import numpy   as np
import pandas  as pd
import seaborn as sns

from analysis.jp.jp06 import lasso_outputs
from analysis.jp.jp10 import slsqp_outputs
from typing           import List
from utils.runtime    import find_project_root

def percent_changes(X    : np.ndarray       = lasso_outputs['X_train'], 
                    sets : List[np.ndarray] = slsqp_outputs['optimized_sets'], 
                    fts  : List[str]        = lasso_outputs['ft_names']):
    '''
    Visualizes the percentage changes in categorical values after optimization.

    Methodology:
        1. Convert the optimized feature sets into a DataFrame.
        2. Calculate the mean of each feature for both original and optimized sets.
        3. Compute and visualize the percentage changes.

    Parameters:
        X    (np.ndarray)       : The transformed training feature set from LASSO feature selection.
        sets (List[np.ndarray]) : The optimized feature sets that meet the desired cost bounds after optimization.
        fts  (List[str])        : A list of feature names after feature selection and transformation by the LASSO model.

    Produces:
        A bar chart saved as a PNG file and displayed on the screen, showing the percent change in categorical features after optimization.
    '''

    # 1: Convert the list of feature arrays into a DataFrame and calculate the means
    optimized_means = pd.DataFrame(sets,        columns = fts).mean()
    original_means  = pd.DataFrame(X.toarray(), columns = fts).mean()

    # 2: Compute the percentage changes
    pct_changes = ((optimized_means - original_means) / original_means).sort_values(ascending = False)
    pct_changes = pct_changes[~pct_changes.index.str.startswith('num_')]

    # 3: Barplot visualization
    bp = sns.barplot(x       = pct_changes, 
                     y       = pct_changes.index, 
                     hue     = pct_changes, 
                     legend  = False,
                     palette = 'twilight')
    
    # Set x-axis labels as formatted percentages
    x_ticks = bp.get_xticks()
    bp.set_xticks(x_ticks)
    bp.set_xticklabels([f'{(x * 100):.0f}%' for x in x_ticks])

    plt.xlabel('Percentage Change (%)')
    plt.ylabel('Feature')
    plt.title('$11$: Percent Change in Categorical Values After Optimization')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/11 - Percent Change in Categorical Features After Optimization.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":

    percent_changes()
