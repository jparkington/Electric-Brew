import matplotlib.pyplot as plt
import numpy as np
import random

from analysis.jp.jp06 import X_train_lasso
from analysis.jp.jp09 import best_estimator
from sklearn.ensemble import RandomForestRegressor
from scipy.optimize   import minimize
from typing           import List, Tuple
from utils.runtime    import find_project_root

def slsqp(X    : np.ndarray            = X_train_lasso, 
          best : RandomForestRegressor = best_estimator) -> Tuple[List[np.ndarray], List[np.ndarray], Tuple[float, float]]:
    '''
    Performs optimization on feature sets and visualizes the distribution of predicted costs for these optimized sets.

    Methodology:
        1. Convert the sparse matrix to a dense array for manipulation.
        2. Select a random subset of samples for optimization.
        3. Define an objective function based on the Random Forest predictions.
        4. Perform optimization on each sample.

    Data Science Concepts:
        • SLSQP (Sequential Least Squares Quadratic Programming):
            - A mathematical optimization algorithm that solves nonlinearly constrained optimization problems.
            - Particularly useful in this context for its ability to handle constraints effectively.
            - We use SLSQP to constrain one specific feature (`num__kwh_delivered`) while allowing other features to vary.

    Parameters:
        X    (np.ndarray)            : The transformed training feature set from LASSO feature selection.
        best (RandomForestRegressor) : The best-fitted Random Forest model from Randomized Search CV.

    Returns:
        all_sets       (List[np.ndarray])    : All feature sets evaluated during the optimization.
        optimized_sets (List[np.ndarray])    : Feature sets that meet the specified cost bounds.
        cost_bounds    (Tuple[float, float]) : Lower and upper bounds for the optimized cost range.
    '''

    # 1: Set constants and prepare data
    X_dense         = X.toarray()
    random_samples  = random.sample(range(len(X_dense)), int(len(X_dense) * 0.05))
    mean_total_cost = best.predict(X).mean()
    cost_bounds     = (mean_total_cost * 0.75, mean_total_cost * 0.95)
    mean_constraint = X_dense[:, 1].mean()
    constraints     = {'type' : 'eq', 
                       'fun'  : lambda features: features[1] - mean_constraint}

    # 2: Define the objective function
    def objective_function(features):
        return best.predict([features])[0]

    # 3: Optimizing and storing results
    all_sets       = []
    optimized_sets = []

    for i in random_samples:

        result = minimize(objective_function, X_dense[i], method = 'SLSQP', constraints = constraints)
        all_sets.append(result.x)

        if cost_bounds[0] <= objective_function(result.x) <= cost_bounds[1]:
            optimized_sets.append(result.x)

    return all_sets, optimized_sets, cost_bounds

all_sets, optimized_sets, cost_bounds = slsqp()


def plot_slsqp(best        : RandomForestRegressor = best_estimator, 
               all_sets    : List[np.ndarray]      = all_sets,
               cost_bounds : Tuple[float, float]   = cost_bounds):
    '''
    Visualizes the distribution of predicted costs for all and optimized feature sets.

    Parameters:
        best        (RandomForestRegressor) : The best-fitted Random Forest model.
        all_sets    (List[np.ndarray])      : List of all feature sets evaluated.
        cost_bounds (Tuple[float, float])   : Lower and upper bounds for the optimized cost range.

    Produces:
        A scatter plot saved as a PNG file and displayed on the screen, showing the distribution of predicted costs.
    '''

    predicted_costs = best.predict(all_sets)

    # Visualizing the results
    plt.scatter(range(len(predicted_costs)), 
                predicted_costs, 
                c    = predicted_costs,
                cmap = 'twilight_shifted',
                vmin = cost_bounds[0], 
                vmax = cost_bounds[1],
                edgecolor = None)

    plt.xlabel('Optimization Sequence')
    plt.ylabel('Predicted Cost')
    plt.title('$11$: Distribution of Predicted Costs in Optimized Feature Sets')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/11 - Distribution of Predicted Costs in Optimized Feature Sets.png')
    plt.savefig(file_path)
    plt.show()

    return optimized_sets


if __name__ == "__main__":
    
    plot_slsqp()
