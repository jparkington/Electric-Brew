import matplotlib.pyplot as plt
import numpy  as np
import pandas as pd

from analysis.jp.jp06        import lasso_outputs
from sklearn.ensemble        import RandomForestRegressor
from sklearn.metrics         import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from typing                  import Tuple
from utils.runtime           import find_project_root, pickle_and_load

def random_forest(X : np.ndarray = lasso_outputs['X_train'], 
                  y : pd.Series  = lasso_outputs['y_train']) -> Tuple[RandomForestRegressor, pd.Series, np.ndarray]:
    '''
    Fits a Random Forest Regressor model using Randomized Search CV for hyperparameter tuning and visualizes predictions.

    Methodology:
        1. Perform a train/test split on the training dataset.
        2. Tune hyperparameters using Randomized Search CV.
        3. Fit the best Random Forest model and make predictions.
        4. Visualize the predicted vs. actual values and calculate R² and MSE.

    Data Science Concepts:
        • Random Forest:
            - An ensemble learning method that operates by constructing multiple decision trees during training.
            - For regression tasks, the output of the random forest is the mean prediction of the individual trees.
        • Hyperparameter Tuning:
            - The process of selecting the set of optimal hyperparameters for a learning algorithm, often using methods like 
              grid search or randomized search.

    Parameters:
        X (np.ndarray) : The transformed training feature set from LASSO feature selection.
        y (pd.Series)  : The training target variable.

    Returns:
        best_estimator (RandomForestRegressor) : The best-fitted Random Forest model.
        y_test_rf      (pd.Series)             : The test target variable for RandomForestRegressor.
        y_pred_rf      (np.ndarray)            : Predicted values by the model on the test set.
    '''

    # 1: Splitting the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 0)

    # 2: Initialize the Random Forest Regressor
    rf = RandomForestRegressor(random_state = 0)

    # Hyperparameter Grid
    hyperparameter_grid = {'n_estimators'      : [1, 2, 4, 8, 16],
                           'max_depth'         : [1, 2, 4, 8, 16],
                           'min_samples_split' : [2, 4, 8, 16],
                           'min_samples_leaf'  : [1, 2, 4, 8]}

    # 3: # Randomized Search with Cross-Validation
    random_search = RandomizedSearchCV(rf, hyperparameter_grid, n_jobs = -1, random_state = 0)
    random_search.fit(X_train, y_train)

    # Predictions using the best model
    best   = random_search.best_estimator_
    y_pred = best.predict(X_test)

    return {'best'   : best, 
            'y_test' : y_test, 
            'y_pred' : y_pred}

random_forest_outputs = pickle_and_load(random_forest, 'jp09.pkl')


def plot_random_forest(best   : RandomForestRegressor = random_forest_outputs['best'], 
                       y_test : pd.Series             = random_forest_outputs['y_test'], 
                       y_pred : np.ndarray            = random_forest_outputs['y_pred']):
    '''
    Visualizes predictions of the Random Forest model compared to actual values. Then calculates R² and MSE.

    Parameters:
        best   (RandomForestRegressor) : The fitted Random Forest model.
        y_test (pd.Series)             : The test target variable for RandomForestRegressor.
        y_pred (np.ndarray)            : Predicted values by the model on the test set.
    
    Produces:
        A scatter plot saved as a PNG file and displayed on the screen, showing the comparison between predicted and actual values.
    '''
    # 4: Visualizing predictions vs. actual values
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], color = '#545B63', linestyle = '--')
    plt.scatter(x = y_test, 
                y = y_pred, 
                c = y_test - y_pred,
                cmap = 'twilight_shifted',
                vmin = -1, 
                vmax = 1,
                edgecolor = None)

    # Text annotation for R² and MSE
    plt.text(0.5, 0.1, 
             (f"R²  ${r2_score(y_test, y_pred):.3f}$\n"
              f"MSE ${mean_squared_error(y_test, y_pred):.3f}$"), 
             fontsize = 12, fontweight = 'bold', linespacing = 1.8,
             bbox = dict(facecolor = '0.3', edgecolor = '0.3', boxstyle = 'round,pad = 0.75', alpha = 0.5),
             ha = 'left', va = 'center', transform = plt.gca().transAxes)

    # Second text annotation for hyperparameters (top left)
    plt.text(0.05, 0.95, 
             (f"Max Depth         ${best.get_params()['max_depth']}$\n"
              f"Min Samples Split ${best.get_params()['min_samples_split']}$\n"
              f"Min Samples Leaf  ${best.get_params()['min_samples_leaf']}$\n"
              f"# of Estimators   ${best.get_params()['n_estimators']}$"), 
             fontsize = 9, fontweight = 'bold', linespacing = 1.3,
             bbox = dict(facecolor = '0.3', edgecolor = '0.3', boxstyle = 'round,pad = 0.75', alpha = 0.5),
             ha = 'left', va = 'top', transform = plt.gca().transAxes)

    plt.colorbar(label = 'Residuals')
    plt.xlabel('Total Cost')
    plt.ylabel('Predicted Values')
    plt.title('$09$: Random Forest - Predictions vs. Actual Values')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/09 - Random Forest Predictions vs Actual Values.png')
    plt.savefig(file_path)
    plt.show()


if __name__ == "__main__":
    
    plot_random_forest()