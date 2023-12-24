import matplotlib.pyplot as plt
import numpy as np
import random
import warnings

from analysis.jp.jp06 import lasso_outputs
from analysis.jp.jp08 import random_forest_outputs
from joblib           import Parallel, delayed
from sklearn.ensemble import RandomForestRegressor
from scipy.optimize   import minimize
from typing           import List, Tuple
from utils.runtime    import find_project_root, pickle_and_load

def slsqp(X    : np.ndarray            = lasso_outputs['X_train'], 
          best : RandomForestRegressor = random_forest_outputs['best']) -> Tuple[List[np.ndarray], List[np.ndarray], Tuple[float, float]]:
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
    cost_bounds     = (mean_total_cost * 0.65, mean_total_cost * 0.85)
    mean_constraint = X_dense[:, 0].mean()
    constraints     = {'type': 'eq', 'fun': lambda features: features[0] - mean_constraint}
    bounds          = [(0.01, None) for _ in range(X.shape[1])]

    # 2: Define the objective function
    def optimize_sample(i):

        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category = RuntimeWarning) # Scipy warns every time the bounds are clipped

            result = minimize(lambda features: best.predict([features])[0], 
                              np.clip(X_dense[i], 0.01, None), 
                              method      = 'SLSQP', 
                              constraints = constraints, 
                              bounds      = bounds)
            
            return result.x, best.predict([result.x])[0]

    # 3: Optimizing and storing results
    results = Parallel(n_jobs = -1)(delayed(optimize_sample)(i) for i in random_samples)

    return {'all_sets'       : [result[0] for result in results], 
            'optimized_sets' : [result[0] for result in results if cost_bounds[0] <= result[1] <= cost_bounds[1]], 
            'cost_bounds'    : cost_bounds}

slsqp_outputs = pickle_and_load(slsqp, 'jp10.pkl')


def plot_slsqp(best        : RandomForestRegressor = random_forest_outputs['best'], 
               all_sets    : List[np.ndarray]      = slsqp_outputs['all_sets'],
               cost_bounds : Tuple[float, float]   = slsqp_outputs['cost_bounds']):
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
    plt.title('$10$: Distribution of Predicted Costs in Optimized Feature Sets')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/10 - Distribution of Predicted Costs in Optimized Feature Sets.png')
    plt.savefig(file_path)
    plt.show()


if __name__ == "__main__":
    
    plot_slsqp()
